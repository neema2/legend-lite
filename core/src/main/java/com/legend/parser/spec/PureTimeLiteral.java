package com.legend.parser.spec;

import java.util.Objects;

/**
 * Pure-language strict-time literal in structured form, port of
 * engine's {@code meta::pure::metamodel::type::StrictTime}
 * (see {@code legend-pure-core/legend-pure-m4/.../primitive/date/TimeFunctions.java}).
 *
 * <h2>Source grammar</h2>
 * From engine's {@code CoreFragmentGrammar.g4}:
 * <pre>{@code
 *   StrictTime : '%' Digit+ (':' Digit+ (':' Digit+ ('.' Digit+)?)?)?
 * }</pre>
 *
 * <p>Hour-only forms ({@code %10}) are <em>not</em> reachable from
 * source via the StrictTime token: the lexer routes any digit-only
 * literal without colons to the {@code DATE} token, where it parses
 * as a {@link PureDateLiteral.Year}. Strict-time literals therefore
 * always carry at least minute precision.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code 10:30}        &rarr; {@link TimeWithMinute}</li>
 *   <li>{@code 10:30:45}     &rarr; {@link TimeWithSecond}</li>
 *   <li>{@code 10:30:45.123} &rarr; {@link TimeWithSubsecond}</li>
 * </ul>
 *
 * <h2>Validation</h2>
 * Components are validated at construction: hour {@code [0,23]},
 * minute {@code [0,59]}, second {@code [0,59]}, subsecond non-empty
 * all-digit. {@link IllegalArgumentException} is thrown for invalid
 * values.
 *
 * <h2>Subsecond representation</h2>
 * Carried as {@link String} for arbitrary-precision round-trip
 * fidelity (same rationale as {@link PureDateLiteral.DateWithSubsecond}).
 */
public sealed interface PureTimeLiteral
        permits PureTimeLiteral.TimeWithMinute,
                PureTimeLiteral.TimeWithSecond,
                PureTimeLiteral.TimeWithSubsecond {

    /**
     * Canonical engine-faithful spelling without the leading
     * {@code %} prefix. Round-trip identity:
     * {@code parse(x.toEngineString())} equals {@code x}.
     */
    String toEngineString();

    record TimeWithMinute(int hour, int minute) implements PureTimeLiteral {
        public TimeWithMinute { validateHour(hour); validateMinute(minute); }
        @Override public String toEngineString() {
            return String.format("%02d:%02d", hour, minute);
        }
    }

    record TimeWithSecond(int hour, int minute, int second) implements PureTimeLiteral {
        public TimeWithSecond {
            validateHour(hour);
            validateMinute(minute);
            validateSecond(second);
        }
        @Override public String toEngineString() {
            return String.format("%02d:%02d:%02d", hour, minute, second);
        }
    }

    record TimeWithSubsecond(int hour, int minute, int second, String subsecond) implements PureTimeLiteral {
        public TimeWithSubsecond {
            validateHour(hour);
            validateMinute(minute);
            validateSecond(second);
            validateSubsecond(subsecond);
        }
        @Override public String toEngineString() {
            return String.format("%02d:%02d:%02d.%s", hour, minute, second, subsecond);
        }
    }

    /**
     * Parse the body of a Pure strict-time literal (text after
     * {@code %}). Throws {@link IllegalArgumentException} for
     * malformed shape or invalid component values.
     */
    static PureTimeLiteral parse(String source) {
        Objects.requireNonNull(source, "source");
        if (source.isEmpty()) {
            throw new IllegalArgumentException("empty time literal");
        }
        int pos = 0;
        int hour = readDigits(source, pos, "hour");
        pos = afterDigits(source, pos);
        if (pos >= source.length() || source.charAt(pos) != ':') {
            throw new IllegalArgumentException(
                    "expected ':' after hour in time literal '" + source + "'");
        }
        pos++;
        int minute = readDigits(source, pos, "minute");
        pos = afterDigits(source, pos);
        if (pos == source.length()) {
            return new TimeWithMinute(hour, minute);
        }
        if (source.charAt(pos) != ':') {
            throw new IllegalArgumentException(
                    "unexpected character after minute at position " + pos
                            + " in '" + source + "'");
        }
        pos++;
        int second = readDigits(source, pos, "second");
        pos = afterDigits(source, pos);
        if (pos == source.length()) {
            return new TimeWithSecond(hour, minute, second);
        }
        if (source.charAt(pos) != '.') {
            throw new IllegalArgumentException(
                    "unexpected character after second at position " + pos
                            + " in '" + source + "'");
        }
        pos++;
        int subStart = pos;
        while (pos < source.length() && isDigit(source.charAt(pos))) pos++;
        if (pos == subStart) {
            throw new IllegalArgumentException(
                    "expected digits after '.' in subsecond in '" + source + "'");
        }
        if (pos != source.length()) {
            throw new IllegalArgumentException(
                    "trailing characters after subsecond: '"
                            + source.substring(pos) + "'");
        }
        return new TimeWithSubsecond(hour, minute, second, source.substring(subStart, pos));
    }

    private static int readDigits(String src, int from, String component) {
        int end = afterDigits(src, from);
        if (end == from) {
            throw new IllegalArgumentException(
                    "expected digits for " + component + " at position " + from
                            + " in '" + src + "'");
        }
        try {
            return Integer.parseInt(src.substring(from, end));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    component + " value out of int range: '" + src.substring(from, end) + "'", e);
        }
    }

    private static int afterDigits(String src, int from) {
        int p = from;
        while (p < src.length() && isDigit(src.charAt(p))) p++;
        return p;
    }

    private static boolean isDigit(char c) { return c >= '0' && c <= '9'; }

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
}
