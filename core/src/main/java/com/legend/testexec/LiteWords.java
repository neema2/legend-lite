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
            return instance(m, instanceClass);
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

    /** One side of a PAIR's text (assertEquals' rule, the database judge's too): each side bare where BOTH hold one
     *  value, else each in brackets. */
    public static String pairSide(List<?> values, List<?> other, @Nullable String instanceClass) {
        return values.size() == 1 && other.size() == 1 ? value(values.get(0), instanceClass)
                : values(values, instanceClass);
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

    /** A JSON document as the database judge's canon writes it in a message (the orchestrator's ruling, 2026-11-17:
     *  both judges write the shared canon's JSON): minified -- whitespace outside strings dropped -- and, where the
     *  root's order is incidental, the root array's elements in their texts' order, comma-joined. */
    public static String canonJson(String json, boolean unorderedRoot) {
        String min = minified(json);
        if (!unorderedRoot || !min.startsWith("[") || !min.endsWith("]")) {
            return min;
        }
        List<String> elements = new java.util.ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 1;
        for (int i = 1; i < min.length() - 1; i++) {
            char c = min.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                } else if (c == '"') {
                    inString = false;
                }
            } else if (c == '"') {
                inString = true;
            } else if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                elements.add(min.substring(start, i));
                start = i + 1;
            }
        }
        if (min.length() > 2) {
            elements.add(min.substring(start, min.length() - 1));
        }
        java.util.Collections.sort(elements);
        return "[" + String.join(", ", elements) + "]";
    }

    /** {@code json} with every whitespace character outside a string dropped. */
    private static String minified(String json) {
        StringBuilder sb = new StringBuilder(json.length());
        boolean inString = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (inString) {
                sb.append(c);
                if (c == '\\' && i + 1 < json.length()) {
                    sb.append(json.charAt(++i));
                } else if (c == '"') {
                    inString = false;
                }
            } else if (c == '"') {
                inString = true;
                sb.append(c);
            } else if (!Character.isWhitespace(c)) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** An instance as lite writes it (the orchestrator's ruling, 2026-11-17: as the product writes one in its results):
     *  its class, then its properties as a JSON object, each value its lite text -- {@code m::P {"name": "a"}}; an
     *  instance with no property to show, its class alone. The class is the one the instance carries on its wire
     *  ({@code __type}), else the side's declared class. */
    static String instance(Map<?, ?> m, @Nullable String instanceClass) {
        Object wireType = m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE);
        String cls = wireType instanceof String t ? t : instanceClass == null ? "?" : instanceClass;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> e : m.entrySet()) {
            if (e.getKey() instanceof String k && k.startsWith("__")) {
                continue;   // an identity field the database carries, no property
            }
            sb.append(sb.length() == 0 ? "" : ", ").append(quoted(String.valueOf(e.getKey()))).append(": ")
                    .append(e.getValue() instanceof List<?> l ? values(l, null) : value(e.getValue()));
        }
        return sb.length() == 0 ? cls : cls + " {" + sb + "}";
    }

    /** A value as an IDENTITY assert names it ({@code assertEq}, {@code assertIs}): an instance the database tracks by
     *  identity as its class and that identity, {@code m::P (instance i1)} -- the two judges' one text, the database
     *  holding no properties for it; any other value as {@link #value} writes it. */
    public static String identity(@Nullable Object v, @Nullable String instanceClass) {
        if (v instanceof Map<?, ?> m && com.legend.testexecutionplan.Verdicts.wireId(v) instanceof String id) {
            Object wireType = m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE);
            String cls = wireType instanceof String t ? t : instanceClass == null ? "?" : instanceClass;
            return cls + " (instance " + id + ")";
        }
        return value(v, instanceClass);
    }
}
