// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.error.AssertSentence;
import com.legend.values.PureDateLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * THE HOST JUDGE'S OWN SENTENCES UNDER LITE'S RULES (the user's judge model, 2026-11-17: lite picks its own best
 * messages; the orchestrator's rulings of that day): a failed assert's message is its {@link AssertSentence} row, each
 * slot a value written as the PRODUCT writes it -- the same text the database judge's statement builds in SQL from the
 * same row, so the two isolated judges agree on the message as on the verdict (their comparison is the drift guard).
 * Test-side text, the judge's: never a writer of the product's output (R3).
 *
 * <p>A value: a String QUOTED JSON-style ({@code "a"}, {@code ""}; a backslash, a quote, a newline, a tab and a
 * carriage return escaped), inside a collection or a row too; an Integer its digits; a Float S311's six decimals (each
 * dialect's {@code FLOAT_TO_TEXT}); a Decimal its plain digits ({@code DECIMAL_TO_TEXT}); a Boolean {@code true} or
 * {@code false}; a date its day, a date-time ISO 8601 in UTC with six fraction digits and {@code Z} (S312,
 * {@code DATETIME_TO_TEXT}); an instance as its id and class, unquoted. A collection is {@code [a, b]}.
 */
public final class LiteWords {

    private LiteWords() {
    }

    /** One value's text in a message. */
    public static String value(@Nullable Object v, @Nullable String instanceClass) {
        if (v instanceof Map<?, ?> m) {
            String id = com.legend.testexecutionplan.Verdicts.wireId(v);
            return "<" + (instanceClass == null ? "?" : instanceClass) + (id == null ? "" : " " + id) + ">";
        }
        return switch (v) {
            case null -> "[]";
            case String s -> quoted(s);
            case Double d -> productFloat(d);
            case Float f -> productFloat(f.doubleValue());
            case BigDecimal d -> d.toPlainString();
            case Long l -> Long.toString(l);
            case Integer i -> Integer.toString(i);
            case BigInteger b -> b.toString();
            case Boolean b -> b.toString();
            case PureDateLiteral d -> dateText(d);
            case List<?> l -> values(l, instanceClass);
            default -> throw new com.legend.error.NotImplementedException(
                    "lite's message text for " + v.getClass().getName() + " is not modeled");
        };
    }

    /** One value's text, no instance class. */
    public static String value(@Nullable Object v) {
        return value(v, null);
    }

    /** A collection's text, {@code [a, b]}. */
    public static String values(List<?> values, @Nullable String instanceClass) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(value(values.get(i), instanceClass));
        }
        return sb.append(']').toString();
    }

    /** A side's text: its one value where it holds one, else its values in brackets. */
    public static String side(List<?> values, @Nullable String instanceClass) {
        return values.size() == 1 ? value(values.get(0), instanceClass) : values(values, instanceClass);
    }

    /** {@code row}'s sentence filled: each slot's text by name. */
    public static String sentence(AssertSentence row, Map<String, String> slots) {
        return row.fill(slots);
    }

    /** A String JSON-style: quoted, a backslash, a quote, a newline, a tab and a carriage return escaped. */
    public static String quoted(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\t' -> sb.append("\\t");
                case '\r' -> sb.append("\\r");
                default -> sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    /** A Float's product text (S311, each dialect's {@code FLOAT_TO_TEXT}): six decimals of the exact value, rounded
     *  half-even, then the trailing zeros and a trailing point trimmed; NaN and the infinities by name. */
    public static String productFloat(double d) {
        if (Double.isNaN(d)) {
            return "NaN";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "Infinity" : "-Infinity";
        }
        String six = new BigDecimal(d).setScale(6, RoundingMode.HALF_EVEN).toPlainString();
        int end = six.length();
        while (six.charAt(end - 1) == '0') {
            end--;
        }
        if (six.charAt(end - 1) == '.') {
            end--;
        }
        String out = six.substring(0, end);
        return out.equals("-0") ? "0" : out;
    }

    /** A date's product text: its day where it has no time of day ({@code yyyy-MM-dd}, or its own text where coarser
     *  than a day), else ISO 8601 in UTC with six fraction digits and {@code Z} (S312). */
    public static String dateText(PureDateLiteral d) {
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
            return com.legend.result.CellText.dayText(d);
        }
        java.time.LocalDateTime t = d.toInstantFloor();
        return String.format(java.util.Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ", t.getYear(),
                t.getMonthValue(), t.getDayOfMonth(), t.getHour(), t.getMinute(), t.getSecond(), t.getNano() / 1000);
    }
}
