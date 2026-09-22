// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** THE platform's JSON reader (F3.1 landed): TdsChecker's validator
 * delegates here (F3.1b), Executor's string decoder is deleted onto
 * {@link #unescapeString} (F3.1d), and the TWO documented exemptions
 * are server/Json (the strict fail-fast HTTP-boundary reader — a
 * different policy on purpose; its decimal path got the audit-18
 * BigDecimal fix in F3.1a) and MongoDBSectionGrammar (token-level,
 * decodes no escapes, loud on floats — F3.1e, recorded at the site).
 * Integers read as Long, decimals as BigDecimal (audit 18: double
 * rounding made distinct Decimals compare equal). */
public final class Json {
    private final String s;
    private int i;
    /** Assert-channel leniency (parseOne only): engine goldens carry
     * typo classes real JSON rejects — a missing comma between array
     * OBJECT elements reads as an implicit comma. Never set for wire
     * parsing (variant/row streams stay strict). */
    private final boolean lenient;

    private Json(String s) {
        this(s, false);
    }

    private Json(String s, boolean lenient) {
        this.s = s;
        this.lenient = lenient;
    }

    public static @com.legend.base.Nullable Object parse(String json) {
        Json p = new Json(json);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.i < p.s.length()) {
            throw new IllegalStateException("trailing JSON at " + p.i);
        }
        return v;
    }

    /** The LEADING value only — real pure parseJSON semantics: a complete
     * root value parses even with trailing text after it (the milestoned
     * graphFetch goldens carry a stray quote after the array; the engine's
     * own parse reads the value and ignores the tail). */
    public static @com.legend.base.Nullable Object parseOne(String json) {
        Json p = new Json(json, true);
        p.ws();
        return p.value();
    }

    private @com.legend.base.Nullable Object value() {
        char c = s.charAt(i);
        return switch (c) {
            case '{' -> obj();
            case '[' -> arr();
            case '"' -> str();
            case 't' -> { i += 4; yield Boolean.TRUE; }
            case 'f' -> { i += 5; yield Boolean.FALSE; }
            case 'n' -> { i += 4; yield null; }
            default -> num();
        };
    }

    private Map<String, Object> obj() {
        Map<String, Object> out = new LinkedHashMap<>();
        i++;
        ws();
        if (s.charAt(i) == '}') {
            i++;
            return out;
        }
        while (true) {
            ws();
            String k = str();
            ws();
            i++;    // ':'
            ws();
            out.put(k, value());
            ws();
            if (s.charAt(i) == ',') {
                i++;
                continue;
            }
            i++;    // '}'
            return out;
        }
    }

    private List<Object> arr() {
        List<Object> out = new ArrayList<>();
        i++;
        ws();
        if (s.charAt(i) == ']') {
            i++;
            return out;
        }
        while (true) {
            ws();
            out.add(value());
            ws();
            if (s.charAt(i) == ',') {
                i++;
                continue;
            }
            if (lenient && s.charAt(i) == '{') {
                continue;   // implicit comma (golden typo class)
            }
            i++;    // ']'
            return out;
        }
    }

    private String str() {
        i++;
        int start = i;
        while (s.charAt(i) != '"') {
            i += s.charAt(i) == '\\' ? 2 : 1;
        }
        String body = s.substring(start, i);
        i++;
        return unescapeString(body);
    }

    /** THE JSON string-escape READ table (F3.1d), decoding an already-
     *  extracted string BODY: named escapes, {@code \-uXXXX}, and
     *  DROP-BACKSLASH for anything unknown (the same terminal rule as
     *  the platform's Pure unescape family). Executor's variant-carrier
     *  decoder was a keep-the-backslash twin of this table and is
     *  deleted — it delegates here. A lone trailing backslash (invalid
     *  JSON, unreachable from a well-formed reader) stays verbatim. */
    /** The CANONICAL text of a parsed JSON value: compact (no whitespace),
     * object keys SORTED, arrays in order, numbers as parsed
     * ({@code BigDecimal.toPlainString}: {@code 1.0} stays {@code 1.0}),
     * strings escaped as a JSON writer does ({@code " \\} and control
     * characters; everything else raw UTF-8). Key order carries no meaning
     * in pure's JSON equality — the engine's own asserts compare
     * structurally — so one fixed order lets bytes decide. */
    public static String canonical(@com.legend.base.Nullable Object v) {
        StringBuilder b = new StringBuilder();
        writeCanonical(v, b);
        return b.toString();
    }

    private static void writeCanonical(@com.legend.base.Nullable Object v, StringBuilder b) {
        if (v == null) {
            b.append("null");
        } else if (v instanceof Map<?, ?> m) {
            java.util.List<String> keys = new ArrayList<>();
            for (Object k : m.keySet()) {
                keys.add(String.valueOf(k));
            }
            java.util.Collections.sort(keys);
            b.append('{');
            boolean first = true;
            for (String k : keys) {
                if (!first) {
                    b.append(',');
                }
                first = false;
                writeString(k, b);
                b.append(':');
                writeCanonical(m.get(k), b);
            }
            b.append('}');
        } else if (v instanceof List<?> l) {
            b.append('[');
            boolean first = true;
            for (Object e : l) {
                if (!first) {
                    b.append(',');
                }
                first = false;
                writeCanonical(e, b);
            }
            b.append(']');
        } else if (v instanceof String str) {
            writeString(str, b);
        } else if (v instanceof java.math.BigDecimal d) {
            b.append(d.toPlainString());
        } else {
            b.append(v);   // Long, Boolean
        }
    }

    private static void writeString(String s, StringBuilder b) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\t' -> b.append("\\t");
                case '\r' -> b.append("\\r");
                case '\b' -> b.append("\\b");
                case '\f' -> b.append("\\f");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        b.append('"');
    }

    public static String unescapeString(String s) {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder b = new StringBuilder(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c != '\\' || i + 1 >= s.length()) {
                b.append(c);
                i++;
                continue;
            }
            char e = s.charAt(i + 1);
            switch (e) {
                case 'n' -> b.append('\n');
                case 't' -> b.append('\t');
                case 'r' -> b.append('\r');
                case 'b' -> b.append('\b');
                case 'f' -> b.append('\f');
                case 'u' -> {
                    b.append((char) Integer.parseInt(
                            s.substring(i + 2, i + 6), 16));
                    i += 4;
                }
                default -> b.append(e);   // drop-backslash
            }
            i += 2;
        }
        return b.toString();
    }

    private Object num() {
        int start = i;
        while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
        String t = s.substring(start, i);
        // Decimal tokens parse as BigDecimal, NOT double (audit 18):
        // two distinct Decimals beyond 17 significant digits round to
        // the SAME double, so a wrong Decimal wire value would compare
        // equal — the JSON bridge must stay as strict as the
        // platform equality owner (PureAsserts.equalScalar keeps
        // Decimal comparison exact, never through double).
        return t.contains(".") || t.contains("e") || t.contains("E")
                ? (Object) new java.math.BigDecimal(t)
                : (Object) Long.parseLong(t);
    }

    private void ws() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
    }
}
