// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.values;

/**
 * The HOST-SIDE half of the pure-literal spelling grammar (F10 proper,
 * docs/F10_CARRIER_DESIGN.md): parses a {@code SqlType.Scalar.LITERAL}
 * cell's text back into its own typed host value. The SQL-side encoder
 * is {@code lowering/LiteralSpelling} — the two are one grammar split
 * only by the layering rule (exec may not import lowering;
 * the sql layer is the STANDALONE library and may not import values —
 * Invariant 6a — so the host half lives beside its value carriers); a spelling
 * form added on either side MUST land on both.
 *
 * <p>The eight forms are mutually disjoint by construction, so parsing is
 * first-character/last-character dispatch, never guessing:
 * quoted → String, {@code true/false} → Boolean, {@code a::b.NAME} → an
 * enum's name, a bare identifier → a type / element name, {@code %…} → temporal,
 * {@code …D} → Decimal, contains a point/exponent → Float, else
 * Integer. Slice 2 carries the NUMERIC family (the mixed-Number
 * carrier); the temporal arm lands with slice 3 (Any migration) — until
 * then a %-spelling reaching this parser is a loud error, not a guess.
 */
public final class LiteralText {

    private LiteralText() {
    }

    /** Parse one LITERAL cell. Null stays null (the empty value). */
    public static @com.legend.base.Nullable Object parse(@com.legend.base.Nullable String s) {
        if (s == null) {
            return null;
        }
        if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
            return pureUnescape(s.substring(1, s.length() - 1));
        }
        if (s.equals("true") || s.equals("false")) {
            return Boolean.valueOf(s);
        }
        int enumDot = s.contains("::") ? s.lastIndexOf('.') : -1;
        if (enumDot > 0) {
            // the seventh form — pure's enum literal Enumeration.NAME (an
            // enumeration always lives in a package, so '::' marks it;
            // before the Decimal arm: a NAME may end in D). The host holds
            // an enum value as its NAME (an enum literal lowers to it, a
            // mapped cell decodes to it) — the enumeration is the
            // declaration's, not the value's, host-side.
            return s.substring(enumDot + 1);
        }
        if (!s.isEmpty() && (Character.isLetter(s.charAt(0)) || s.charAt(0) == '_')
                && s.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '_')
                && !s.equals("true") && !s.equals("false")) {
            // the eighth form — a TYPE or ELEMENT value's bare simple name
            // (String, Car, a Mapping's name — the wire convention for type
            // and element values): the host holds it as the name
            return s;
        }
        if (s.startsWith("%")) {
            // slice 3: the temporal arm — the body after % parses on
            // the ONE host temporal carrier (PureDateLiteral, the
            // engine's own progressive-component grammar)
            return PureDateLiteral.parse(s.substring(1));
        }
        if (s.endsWith("D") || s.endsWith("d")) {
            return new java.math.BigDecimal(s.substring(0, s.length() - 1));
        }
        if (s.contains(".") || s.contains("e") || s.contains("E")) {
            return Double.valueOf(s);
        }
        return Long.valueOf(s);
    }

    /** EXACT inverse of the encoder's string framing
     * ({@code LiteralSpelling.literal}: backslash then quote escape —
     * the PURE literal table, deliberately NOT JSON's): one
     * left-to-right pass over backslash-backslash and backslash-quote;
     * every other character (real newlines included) rides raw inside
     * the quotes. */
    private static String pureUnescape(String s) {
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(i + 1);
                if (n == '\\' || n == '\'') {
                    b.append(n);
                    i++;
                    continue;
                }
            }
            b.append(c);
        }
        return b.toString();
    }
}
