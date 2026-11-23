// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** THE HOST JUDGE'S VARIANT DOCUMENT (item 3, register S401; the user, 2026-11-23: "Keep the class but it's only used
 *  in host judge right? Have to make sure not used in database"): only the host judge reads it -- made by its side's
 *  reading ({@code HostJudgments}), compared by its equality ({@code Equality}), printed by the records' printer
 *  ({@code LiteWords}); the database judge compares Variants in SQL ({@code SqlFn.JSON_EQUAL}) and never reaches it
 *  ({@code ArchitectureTest.onlyTheHostJudgeReadsAVariantDocument}).
 *
 *  A Variant's value as the host compares it: its JSON tree ({@code tree}: an object's keys in their written order,
 *  an integer a BigInteger, a number with a fraction or an exponent a Double) and its minified text ({@code text},
 *  its order among Variants and its record). A distinct type, so a Variant meets a Variant alone. */
public record JsonDocument(@com.legend.base.Nullable Object tree, String text) {
    /** Two Variants the same document as its parsed text (register S401, the user's tenet of 2026-11-23: lite's own
     *  rule where Pure's identity says nothing useful, the leanest one -- DuckDB's {@code json(a) = json(b)}):
     *  spacing aside, an object's keys in their order, an array's elements in order, an integer against an integer,
     *  a fraction against a fraction as a double ({@code 1.50} is {@code 1.5}, {@code 1} is not {@code 1.0}), a
     *  Boolean never a number, JSON null a value of its own. */
    boolean sameAs(JsonDocument o) {
        return document(tree, o.tree);
    }

    private static boolean document(@com.legend.base.Nullable Object x, @com.legend.base.Nullable Object y) {
        if (x == null || y == null) {
            return x == y;
        }
        if (x instanceof Map<?, ?> mx) {
            if (!(y instanceof Map<?, ?> my) || !new ArrayList<>(mx.keySet()).equals(new ArrayList<>(my.keySet()))) {
                return false;
            }
            for (Object k : mx.keySet()) {
                if (!document(mx.get(k), my.get(k))) {
                    return false;
                }
            }
            return true;
        }
        if (x instanceof List<?> lx) {
            if (!(y instanceof List<?> ly) || lx.size() != ly.size()) {
                return false;
            }
            for (int i = 0; i < lx.size(); i++) {
                if (!document(lx.get(i), ly.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return switch (x) {
            case java.math.BigInteger i -> y instanceof java.math.BigInteger j && i.equals(j);
            case Double d -> y instanceof Double e && d.equals(e);
            default -> x.equals(y);   // a String, a Boolean: never equal to another kind
        };
    }

    /** A Variant side's values -- each carried as its JSON text -- as documents (an integer a BigInteger, a fraction a
     *  Double): the host judge compares two Variants as the same parsed document (register S401), as the database's
     *  {@code JSON_EQUAL} does on the database judge's side. The host judge's alone
     *  ({@code ArchitectureTest.onlyTheHostJudgeReadsAVariantDocument}). */
    static List<Object> all(List<Object> values) {
        List<Object> out = new java.util.ArrayList<>(values.size());
        for (Object v : values) {
            Object tree = v instanceof String s ? com.legend.sql.Json.parse(s) : v;
            out.add(new JsonDocument(jsonNumbers(tree), v instanceof String s ? LiteWords.canonJson(s, false)
                    : com.legend.sql.Json.canonical(tree)));
        }
        return out;
    }

    private static @com.legend.base.Nullable Object jsonNumbers(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> null;
            case java.math.BigDecimal d -> d.doubleValue();
            case java.math.BigInteger i -> i;
            case Long l -> java.math.BigInteger.valueOf(l);
            case Integer i -> java.math.BigInteger.valueOf(i);
            case Number n -> n.doubleValue();
            case java.util.Map<?, ?> m -> {
                java.util.Map<Object, @com.legend.base.Nullable Object> o = new java.util.LinkedHashMap<>();
                m.forEach((k, x) -> o.put(k, jsonNumbers(x)));
                yield o;
            }
            case List<?> l -> {
                List<@com.legend.base.Nullable Object> o = new java.util.ArrayList<>(l.size());
                l.forEach(x -> o.add(jsonNumbers(x)));
                yield o;
            }
            default -> v;
        };
    }
}
