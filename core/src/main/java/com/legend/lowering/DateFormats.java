// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/** Date-format pattern translation for the scalar lowering rules. */
final class DateFormats {

    private DateFormats() {
    }

    /**
     * Java SimpleDateFormat pattern -> strftime, longest token first. Values
     * are UTC throughout, so the ZONE directives are literals: Z prints the
     * +0000 offset, X the ISO 'Z'.
     */
    static String javaDateToStrftime(String pattern) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < pattern.length()) {
            if (pattern.charAt(i) == '"') {
                int close = pattern.indexOf('"', i + 1);
                if (close < 0) {
                    throw new IllegalStateException("unterminated quote in date pattern: " + pattern);
                }
                out.append(pattern, i + 1, close);
                i = close + 1;
                continue;
            }
            String rest = pattern.substring(i);
            String[][] tokens = {
                    {"yyyy", "%Y"}, {"SSS", "%g"}, {"MM", "%m"}, {"dd", "%d"},
                    {"HH", "%H"}, {"hh", "%I"}, {"mm", "%M"}, {"ss", "%S"},
                    {"h", "%-I"}, {"a", "%p"}, {"Z", "+0000"}, {"X", "Z"},
            };
            boolean matched = false;
            for (String[] t : tokens) {
                if (rest.startsWith(t[0])) {
                    out.append(t[1]);
                    i += t[0].length();
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                char ch = pattern.charAt(i);
                // A pattern LETTER outside the token table would silently
                // pass through as literal text ('MMM' -> '03M'; audit) —
                // loud instead; punctuation/separators pass.
                if (Character.isLetter(ch)) {
                    throw new IllegalStateException("unsupported date-format"
                            + " token '" + ch + "' in pattern '" + pattern + "'");
                }
                out.append(ch);
                i++;
            }
        }
        return out.toString();
    }
}
