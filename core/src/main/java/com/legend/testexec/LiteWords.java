// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.error.AssertKind;
import com.legend.values.PureDateLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * THE HOST JUDGE'S OWN SENTENCES UNDER LITE'S RULES (the user's judge model, 2026-11-17: lite picks its own best
 * messages; the orchestrator's rulings of that day): a failed assert's message is its {@link AssertKind} row, each
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
            case Double d -> messageFloat(d);
            case Float f -> messageFloat(f.doubleValue());
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
    public static String sentence(AssertKind row, Map<String, String> slots) {
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

    /** A Float's text in a MESSAGE (the orchestrator's ruling, 2026-11-17: a failure message is a diagnostic, so a
     *  Float is written at full round-trip precision, not S311's six decimals), the same text the database judge's
     *  {@code FLOAT_PRINT} writes, normalized: Java's shortest round-trip digits; where Java writes an exponent, the
     *  value laid out positionally when it fits a DECIMAL(38,18) exactly (at most 18 fraction and 20 integer digits),
     *  else Java's exponent form ({@code 1.0E-300}); a negative zero keeps its sign ({@code -0.0}); NaN and the
     *  infinities as Java names them. */
    public static String messageFloat(double d) {
        String shortest = Double.toString(d);
        if (shortest.indexOf('E') < 0) {
            return shortest;
        }
        BigDecimal exact = new BigDecimal(shortest).stripTrailingZeros();
        int fraction = Math.max(exact.scale(), 0);
        int integer = exact.precision() - exact.scale();
        if (fraction > 18 || integer > 20) {
            return shortest;
        }
        String plain = exact.toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
    }

    /** A value's KIND as lite's sentence names it where two values' texts tie
     *  ({@link com.legend.error.AssertKind#EQUALS_KINDS}): Integer, Float, Decimal, String, Boolean, StrictDate,
     *  DateTime, an instance's class; a collection its elements' one kind, else Any. */
    public static String kind(@Nullable Object v, @Nullable String instanceClass) {
        return switch (v) {
            case null -> "Nil";
            case Long l -> "Integer";
            case Integer i -> "Integer";
            case BigInteger b -> "Integer";
            case Double d -> "Float";
            case Float f -> "Float";
            case BigDecimal d -> "Decimal";
            case String s -> "String";
            case Boolean b -> "Boolean";
            case PureDateLiteral d -> d.precision().atLeast(PureDateLiteral.Precision.HOUR) ? "DateTime"
                    : d.precision().atLeast(PureDateLiteral.Precision.DAY) ? "StrictDate" : "Date";
            case Map<?, ?> m -> m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE) instanceof String t ? t
                    : instanceClass == null ? "Any" : instanceClass;
            case List<?> l -> {
                java.util.Set<String> kinds = new java.util.LinkedHashSet<>();
                for (Object e : l) {
                    kinds.add(kind(e, instanceClass));
                }
                yield kinds.size() == 1 ? kinds.iterator().next() : kinds.isEmpty() ? "Nil" : "Any";
            }
            default -> "Any";
        };
    }

    /** A side's kind: its one value's, else its values'. */
    public static String sideKind(List<?> values, @Nullable String instanceClass) {
        return values.size() == 1 ? kind(values.get(0), instanceClass) : kind(values, instanceClass);
    }

    /** {@code EQUALS} over two written sides, or {@code EQUALS_KINDS} where their texts tie (the orchestrator's ruling,
     *  2026-11-17): each side's kind after it, and where both are DateTimes, each one's precision too. */
    public static String equalsSentence(String e, String a, java.util.function.Supplier<String> eKind,
            java.util.function.Supplier<String> aKind, @Nullable Object eValue, @Nullable Object aValue) {
        if (!e.equals(a)) {
            return com.legend.error.AssertKind.EQUALS.fill(Map.of("expected", e, "actual", a));
        }
        String ek = eKind.get();
        String ak = aKind.get();
        if (ek.equals(ak) && ek.equals("DateTime") && eValue instanceof PureDateLiteral ed
                && aValue instanceof PureDateLiteral ad) {
            ek = ek + ", " + ed.precision().name().toLowerCase(java.util.Locale.ROOT);
            ak = ak + ", " + ad.precision().name().toLowerCase(java.util.Locale.ROOT);
        }
        return com.legend.error.AssertKind.EQUALS_KINDS.fill(Map.of("expected", e, "expectedKind", ek,
                "actual", a, "actualKind", ak));
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

    /** An instance as lite writes it (the orchestrator's rulings, 2026-11-17: the product's own compact JSON, null for
     *  an empty property, class-prefixed): {@code m::P {"name":"a","age":null}}; an
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
            sb.append(sb.length() == 0 ? "" : ",").append(quoted(String.valueOf(e.getKey()))).append(':')
                    .append(jsonValue(e.getValue()));
        }
        return sb.length() == 0 ? cls : cls + " {" + sb + "}";
    }

    /** A property's value in an instance's compact JSON: an empty one {@code null}, a collection its values comma-joined
     *  in brackets, any other its lite text. */
    private static String jsonValue(@Nullable Object v) {
        if (v == null || v instanceof List<?> l && l.isEmpty()) {
            return "null";
        }
        if (v instanceof List<?> l) {
            StringBuilder sb = new StringBuilder("[");
            for (Object e : l) {
                sb.append(sb.length() == 1 ? "" : ",").append(jsonValue(e));
            }
            return sb.append(']').toString();
        }
        return value(v);
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

    /** Whether a side's values arrived as JSON TEXT (a class value carried as its JSON document). */
    public static boolean jsonCarried(List<Object> raw) {
        return !raw.isEmpty() && raw.stream().allMatch(v -> v instanceof String t
                && (t.startsWith("{") || t.startsWith("[")));
    }

    /** A side carried as JSON text, written as that JSON minified (the shared canon's, the orchestrator's ruling 5 of
     *  2026-11-17), each side bare where both hold one value, else bracketed; a side not so carried as
     *  {@link #pairSide} writes it. */
    public static String carriedSide(List<Object> raw, List<Object> other, @Nullable String instanceClass) {
        if (!jsonCarried(raw)) {
            return pairSide(raw, other, instanceClass);
        }
        List<String> texts = new java.util.ArrayList<>();
        for (Object v : raw) {
            texts.add(canonJson((String) v, false));
        }
        return raw.size() == 1 && other.size() == 1 ? texts.get(0) : "[" + String.join(", ", texts) + "]";
    }
}
