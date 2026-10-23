// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The JSON a Deephaven plan is written in (docs/DEEPHAVEN_PLAN_2026_10_22.md §0, DH1): Deephaven's table-operation batch
 * in its protocol's JSON form, so the connector reads it into the request it sends and decides nothing. Names here are
 * the protocol's field names (an interface's facts); no Deephaven code or text is copied.
 */
final class DeephavenJson {

    private DeephavenJson() {
    }

    /** The scope-ticket prefix a server's named table is fetched by ({@code s/<name>}). */
    private static final String SCOPE = "s/";

    /** The ticket the connector puts the plan's parameters table in place of (DH5): a one-row table of the bound values,
     *  column {@code __p<i>} the statement's i-th placeholder, uploaded typed and never written into formula text. */
    static final String PARAMETERS_TICKET = "legend/parameters";

    /** A Deephaven name: what a column or a table may be called (letters, digits, {@code _} and {@code $}, not a
     *  digit first). */
    private static final Pattern NAME = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*");

    /** Names the formula language gives a meaning of its own (Java's keywords, the literals, the row variables
     *  {@code i}, {@code ii} and {@code k}): never a column's. The server's own validator is the judge; this refuses
     *  what is known first. */
    private static final Set<String> RESERVED = Set.of("abstract", "assert", "boolean", "break", "byte", "case",
            "catch", "char", "class", "const", "continue", "default", "do", "double", "else", "enum", "extends", "final",
            "finally", "float", "for", "goto", "if", "implements", "import", "instanceof", "int", "interface", "long",
            "native", "new", "package", "private", "protected", "public", "return", "short", "static", "strictfp",
            "super", "switch", "synchronized", "this", "throw", "throws", "transient", "try", "void", "volatile", "while",
            "true", "false", "null", "in", "not", "i", "ii", "k");

    /** Whether {@code name} is one Deephaven holds as written. */
    static boolean holdsName(String name) {
        return NAME.matcher(name).matches() && !RESERVED.contains(name);
    }

    /** {@code name} as a Deephaven name, refused by name when the server could not hold it. */
    static String name(String name, String what) {
        if (!holdsName(name)) {
            throw new DialectCapability(what + " '" + name + "' reached Deephaven, whose names are letters, digits, _"
                    + " and $ (not a digit first, not a reserved word): no shape for it yet");
        }
        return name;
    }

    /** A JSON string. */
    static String string(String s) {
        StringBuilder b = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.append('"').toString();
    }

    /** A JSON array of strings. */
    static String strings(List<String> values) {
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(string(values.get(i)));
        }
        return b.append(']').toString();
    }

    /** A table reference to the server's table {@code name} (its scope ticket, in the protocol's base64 bytes). */
    static String scopeTable(String name) {
        return ticket(SCOPE + name);
    }

    /** The table reference the connector replaces by the uploaded parameters table. */
    static String parametersTable() {
        return ticket(PARAMETERS_TICKET);
    }

    private static String ticket(String bytes) {
        return "{\"ticket\": {\"ticket\": \"" + Base64.getEncoder().encodeToString(bytes.getBytes(StandardCharsets.UTF_8))
                + "\"}}";
    }

    /** A table reference to the batch's {@code offset}-th operation's result. */
    static String offset(int offset) {
        return "{\"batchOffset\": " + offset + "}";
    }
}
