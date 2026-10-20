// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.json.Json;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The reference's expected answer: JSON with the facts the comparison needs and JSON cannot carry -- a number's
 * exactness, a date's kind, and for a list the RANKS its order fixes (items of one rank may come in any order; an
 * unordered list is one rank).
 */
public sealed interface OVal {

    record Null() implements OVal {
    }

    record Str(String value) implements OVal {
    }

    record Bool(boolean value) implements OVal {
    }

    /** A number; {@code exact} for integers (compared exactly), else compared within a relative tolerance. */
    record Num(BigDecimal value, boolean exact) implements OVal {
    }

    record DateV(LocalDate value) implements OVal {
    }

    record DateTimeV(LocalDateTime value) implements OVal {
    }

    record Obj(LinkedHashMap<String, OVal> fields) implements OVal {
    }

    /** A list; {@code ranks.get(i)} the rank of item {@code i} under the order the document fixes. */
    record Lst(List<OVal> items, List<Integer> ranks) implements OVal {
        static Lst unordered(List<OVal> items) {
            List<Integer> r = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                r.add(0);
            }
            return new Lst(items, r);
        }
    }

    OVal NULL = new Null();

    // ---- comparison against lite's answer ----

    /** The first difference between {@code expected} and {@code actual}, or null where they agree. */
    static String diff(OVal expected, Json.Node actual, String path) {
        switch (expected) {
            case Null n -> {
                return actual instanceof Json.Null ? null : path + ": expected null, got " + Json.toCompact(actual);
            }
            case Str s -> {
                return actual instanceof Json.Str a && a.value().equals(s.value()) ? null
                        : path + ": expected \"" + s.value() + "\", got " + Json.toCompact(actual);
            }
            case Bool b -> {
                return actual instanceof Json.Bool a && a.value() == b.value() ? null
                        : path + ": expected " + b.value() + ", got " + Json.toCompact(actual);
            }
            case Num n -> {
                if (!(actual instanceof Json.Num a)) {
                    return path + ": expected " + n.value().toPlainString() + ", got " + Json.toCompact(actual);
                }
                BigDecimal got = numberOf(a);
                if (n.exact()) {
                    return got.compareTo(n.value()) == 0 ? null
                            : path + ": expected " + n.value().toPlainString() + ", got " + Json.toCompact(actual);
                }
                double e = n.value().doubleValue();
                double g = got.doubleValue();
                double tol = 1e-9 * Math.max(1.0, Math.max(Math.abs(e), Math.abs(g)));
                return Math.abs(e - g) <= tol ? null
                        : path + ": expected ~" + n.value().toPlainString() + ", got " + Json.toCompact(actual);
            }
            case DateV d -> {
                return actual instanceof Json.Str a && a.value().equals(d.value().toString()) ? null
                        : path + ": expected date " + d.value() + ", got " + Json.toCompact(actual);
            }
            case DateTimeV d -> {
                if (actual instanceof Json.Str a) {
                    LocalDateTime got = parseDateTime(a.value());
                    if (got != null && got.equals(d.value())) {
                        return null;
                    }
                }
                return path + ": expected date-time " + d.value() + ", got " + Json.toCompact(actual);
            }
            case Obj o -> {
                if (!(actual instanceof Json.Obj a)) {
                    return path + ": expected an object, got " + Json.toCompact(actual);
                }
                List<String> ek = new ArrayList<>(o.fields().keySet());
                List<String> ak = new ArrayList<>(a.fields().keySet());
                if (!ek.equals(ak)) {
                    return path + ": expected keys " + ek + ", got " + ak;
                }
                for (Map.Entry<String, OVal> e : o.fields().entrySet()) {
                    String d = diff(e.getValue(), a.fields().get(e.getKey()), path + "." + e.getKey());
                    if (d != null) {
                        return d;
                    }
                }
                return null;
            }
            case Lst l -> {
                if (!(actual instanceof Json.Arr a)) {
                    return path + ": expected a list, got " + Json.toCompact(actual);
                }
                if (a.items().size() != l.items().size()) {
                    return path + ": expected " + l.items().size() + " items, got " + a.items().size() + ": "
                            + Json.toCompact(actual);
                }
                // each run of one rank: the same items, in any order
                int i = 0;
                while (i < l.items().size()) {
                    int j = i;
                    while (j < l.items().size() && l.ranks().get(j).equals(l.ranks().get(i))) {
                        j++;
                    }
                    String d = sameMultiset(l.items().subList(i, j), a.items().subList(i, j), path + "[" + i + ".." + j
                            + ")");
                    if (d != null) {
                        return d;
                    }
                    i = j;
                }
                return null;
            }
        }
    }

    private static String sameMultiset(List<OVal> expected, List<Json.Node> actual, String path) {
        if (expected.size() == 1) {
            return diff(expected.get(0), actual.get(0), path);
        }
        boolean[] used = new boolean[actual.size()];
        for (OVal e : expected) {
            boolean found = false;
            String firstDiff = null;
            for (int k = 0; k < actual.size(); k++) {
                if (used[k]) {
                    continue;
                }
                String d = diff(e, actual.get(k), path);
                if (d == null) {
                    used[k] = true;
                    found = true;
                    break;
                }
                if (firstDiff == null) {
                    firstDiff = d;
                }
            }
            if (!found) {
                return path + ": no answer item matches the expected " + render(e) + " (nearest: " + firstDiff + ")";
            }
        }
        return null;
    }

    static BigDecimal numberOf(Json.Num n) {
        if (n.decimalValue() != null) {
            return n.decimalValue();
        }
        if (n.isInteger()) {
            return BigDecimal.valueOf(n.longValue());
        }
        return new BigDecimal(Double.toString(n.doubleValue()));
    }

    DateTimeFormatter LENIENT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart().appendLiteral('T').optionalEnd()
            .optionalStart().appendLiteral(' ').optionalEnd()
            .optionalStart().appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .optionalEnd()
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .toFormatter(java.util.Locale.ROOT);

    /** A date-time in any of the spellings the database paths write ({@code 2024-06-01T10:00:00.000000000},
     *  {@code 2024-06-01 10:00:00}, {@code 2024-06-01}); null when the text is none of them. */
    static LocalDateTime parseDateTime(String s) {
        try {
            return LocalDateTime.parse(s, LENIENT);
        } catch (java.time.format.DateTimeParseException e) {
            return null;
        }
    }

    /** The expected value as JSON text, for messages and goldens. */
    static String render(OVal v) {
        return switch (v) {
            case Null n -> "null";
            case Str s -> Json.toCompact(s.value());
            case Bool b -> Boolean.toString(b.value());
            case Num n -> n.value().toPlainString();
            case DateV d -> "\"" + d.value() + "\"";
            case DateTimeV d -> "\"" + d.value() + "\"";
            case Obj o -> {
                StringBuilder b = new StringBuilder("{");
                boolean first = true;
                for (Map.Entry<String, OVal> e : o.fields().entrySet()) {
                    if (!first) {
                        b.append(',');
                    }
                    first = false;
                    b.append(Json.toCompact(e.getKey())).append(':').append(render(e.getValue()));
                }
                yield b.append('}').toString();
            }
            case Lst l -> {
                StringBuilder b = new StringBuilder("[");
                for (int i = 0; i < l.items().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    b.append(render(l.items().get(i)));
                }
                yield b.append(']').toString();
            }
        };
    }
}
