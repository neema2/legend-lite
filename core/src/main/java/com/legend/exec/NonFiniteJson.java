// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A database's JSON text as JSON nodes where its writer spells a double that is not finite with a bare token
 * ({@code NaN}, {@code Infinity}, {@code -Infinity}: a dialect says so, {@code SqlDialect.jsonSpellsNonFiniteNumbers}), which
 * JSON has no number for (a float divided by a zero inside a list, the study's §12.79); every other value read as
 * {@link com.legend.json.Json} reads it, the non-finite number a {@link com.legend.json.Json.Num} of that double
 * carrying the token, which {@code Json.toCompact} writes back as read. The typed reader ({@link JdbcDecode}) reads
 * such a database's JSON cell by it -- moved here from the parity harnesses' test sources on 2026-10-27 (the result
 * boundary program's phase 5), when the old executor that handed them the driver's own JSON object was deleted --
 * and the parity grid and the Cypher judge read a Variant's text by it.
 */
public final class NonFiniteJson {
    private final String src;
    private int at;

    private NonFiniteJson(String src) {
        this.src = src;
    }

    public static com.legend.json.Json.Node parse(String text) {
        NonFiniteJson p = new NonFiniteJson(text);
        com.legend.json.Json.Node n = p.value();
        p.ws();
        if (p.at != text.length()) {
            throw new IllegalStateException("a database's JSON: trailing text at " + p.at + " of " + text);
        }
        return n;
    }

    private void ws() {
        while (at < src.length() && Character.isWhitespace(src.charAt(at))) {
            at++;
        }
    }

    private com.legend.json.Json.Node value() {
        ws();
        if (at >= src.length()) {
            throw new IllegalStateException("a database's JSON: a value expected at the end of " + src);
        }
        char c = src.charAt(at);
        if (c == '[') {
            at++;
            List<com.legend.json.Json.Node> items = new ArrayList<>();
            ws();
            if (src.charAt(at) == ']') {
                at++;
                return new com.legend.json.Json.Arr(items);
            }
            while (true) {
                items.add(value());
                ws();
                char d = src.charAt(at++);
                if (d == ']') {
                    return new com.legend.json.Json.Arr(items);
                }
                if (d != ',') {
                    throw new IllegalStateException("a database's JSON: ',' or ']' expected at " + (at - 1) + " of "
                            + src);
                }
            }
        }
        if (c == '{') {
            at++;
            LinkedHashMap<String, com.legend.json.Json.Node> fields = new LinkedHashMap<>();
            ws();
            if (src.charAt(at) == '}') {
                at++;
                return new com.legend.json.Json.Obj(fields);
            }
            while (true) {
                ws();
                String key = string();
                ws();
                if (src.charAt(at++) != ':') {
                    throw new IllegalStateException("a database's JSON: ':' expected at " + (at - 1) + " of " + src);
                }
                fields.put(key, value());
                ws();
                char d = src.charAt(at++);
                if (d == '}') {
                    return new com.legend.json.Json.Obj(fields);
                }
                if (d != ',') {
                    throw new IllegalStateException("a database's JSON: ',' or '}' expected at " + (at - 1) + " of "
                            + src);
                }
            }
        }
        if (c == '"') {
            return new com.legend.json.Json.Str(string());
        }
        for (Map.Entry<String, Double> token : NON_FINITE) {
            if (src.startsWith(token.getKey(), at)) {
                at += token.getKey().length();
                double v = token.getValue();
                return new com.legend.json.Json.Num(0L, v, false, null, token.getKey());
            }
        }
        // a literal (true, false, null) or a number: the token up to the next delimiter, read as JSON reads it
        int from = at;
        while (at < src.length() && ",]} \t\r\n".indexOf(src.charAt(at)) < 0) {
            at++;
        }
        return com.legend.json.Json.parse(src.substring(from, at));
    }

    /** A JSON string at {@code at}: its text between the quotes, read as JSON reads it. */
    private String string() {
        int from = at;
        if (src.charAt(at) != '"') {
            throw new IllegalStateException("a database's JSON: a string expected at " + at + " of " + src);
        }
        at++;
        while (src.charAt(at) != '"') {
            at += src.charAt(at) == '\\' ? 2 : 1;
        }
        at++;
        return ((com.legend.json.Json.Str) com.legend.json.Json.parse(src.substring(from, at))).value();
    }

    /** The spellings of a double that is not finite (the negative first, so {@code -Infinity} is not read
     *  as a number). */
    private static final List<Map.Entry<String, Double>> NON_FINITE = List.of(
            Map.entry("-Infinity", Double.NEGATIVE_INFINITY), Map.entry("Infinity", Double.POSITIVE_INFINITY),
            Map.entry("NaN", Double.NaN));
}
