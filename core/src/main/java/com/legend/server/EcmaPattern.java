// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import java.util.Locale;
import java.util.Map;

/**
 * A JSON SCHEMA PATTERN IN THE PLATFORM'S REGEX DIALECT (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4): a schema's
 * {@code pattern} is ECMA-262's (JSON Schema 2020-12 §6.4), the platform's {@code regexpLike} pattern is RE2's (the
 * dialects translate RE2 to each database's own, e.g. {@code sql/dialect/Postgres.pattern}). The two agree on most of
 * a pattern; this writes the rest in RE2, or says why it cannot be:
 * <ul>
 *   <li>written otherwise: {@code \}{@code uXXXX} and {@code \}{@code u{X...}} (a surrogate pair one code point) as
 *       {@code \x{...}}, {@code \cX} and {@code \0} as the character they are, an identity escape of a letter
 *       ({@code \A}, {@code \z}: anchors in RE2, the letter in ECMA) as the letter, {@code \b} in a class (a
 *       backspace) as {@code \x08}, {@code [^]} (any character) and {@code []} (none), a named group as a group,
 *       {@code \p{Script=Greek}} and {@code \p{Letter}} as RE2 spells them, a {@code [} inside a class escaped;</li>
 *   <li>not carried: a lookahead or lookbehind, a backreference (RE2 has neither, for linear time), a repetition count
 *       past RE2's 1,000, a Unicode property RE2 does not name.</li>
 * </ul>
 * Character scanning only: no regular expression reads a pattern here.
 */
final class EcmaPattern {

    private EcmaPattern() {
    }

    /** A pattern in RE2, or the reason it has none ({@code re2} null). */
    record Translation(@com.legend.base.Nullable String re2, @com.legend.base.Nullable String why) {
    }

    /** RE2's largest repetition count. */
    private static final int MAX_REPEAT = 1000;

    /** ECMA's long general categories, as RE2 names them. */
    private static final Map<String, String> CATEGORIES = Map.ofEntries(
            Map.entry("Letter", "L"), Map.entry("Uppercase_Letter", "Lu"),
            Map.entry("Lowercase_Letter", "Ll"), Map.entry("Titlecase_Letter", "Lt"),
            Map.entry("Modifier_Letter", "Lm"), Map.entry("Other_Letter", "Lo"), Map.entry("Mark", "M"),
            Map.entry("Nonspacing_Mark", "Mn"), Map.entry("Spacing_Mark", "Mc"), Map.entry("Enclosing_Mark", "Me"),
            Map.entry("Number", "N"), Map.entry("Decimal_Number", "Nd"), Map.entry("Letter_Number", "Nl"),
            Map.entry("Other_Number", "No"), Map.entry("Punctuation", "P"), Map.entry("Connector_Punctuation", "Pc"),
            Map.entry("Dash_Punctuation", "Pd"), Map.entry("Open_Punctuation", "Ps"),
            Map.entry("Close_Punctuation", "Pe"), Map.entry("Initial_Punctuation", "Pi"),
            Map.entry("Final_Punctuation", "Pf"), Map.entry("Other_Punctuation", "Po"), Map.entry("Symbol", "S"),
            Map.entry("Math_Symbol", "Sm"), Map.entry("Currency_Symbol", "Sc"), Map.entry("Modifier_Symbol", "Sk"),
            Map.entry("Other_Symbol", "So"), Map.entry("Separator", "Z"), Map.entry("Space_Separator", "Zs"),
            Map.entry("Line_Separator", "Zl"), Map.entry("Paragraph_Separator", "Zp"), Map.entry("Other", "C"),
            Map.entry("Control", "Cc"), Map.entry("Format", "Cf"), Map.entry("Surrogate", "Cs"),
            Map.entry("Private_Use", "Co"));

    /** RE2's general categories. */
    private static final java.util.Set<String> RE2_CATEGORIES = java.util.Set.of("C", "Cc", "Cf", "Co", "Cs", "L", "Ll",
            "Lm", "Lo", "Lt", "Lu", "M", "Mc", "Me", "Mn", "N", "Nd", "Nl", "No", "P", "Pc", "Pd", "Pe", "Pf", "Pi",
            "Po", "Ps", "S", "Sc", "Sk", "Sm", "So", "Z", "Zl", "Zp", "Zs");

    /** The scripts a schema's pattern names, as RE2 names them (Unicode's names): the living scripts, not every one
     *  RE2 knows; another is noted, not guessed. */
    private static final java.util.Set<String> RE2_SCRIPTS = java.util.Set.of("Arabic", "Armenian", "Bengali",
            "Bopomofo", "Braille", "Cherokee", "Common", "Coptic", "Cyrillic", "Devanagari", "Ethiopic", "Georgian",
            "Gothic", "Greek", "Gujarati", "Gurmukhi", "Han", "Hangul", "Hebrew", "Hiragana", "Inherited", "Kannada",
            "Katakana", "Khmer", "Lao", "Latin", "Malayalam", "Mongolian", "Myanmar", "Ogham", "Oriya", "Runic",
            "Sinhala", "Syriac", "Tamil", "Telugu", "Thaana", "Thai", "Tibetan", "Tifinagh", "Yi");

    /** The keys a property names a category or a script by. */
    private static final java.util.Set<String> PROPERTY_KEYS = java.util.Set.of("General_Category", "gc", "Script",
            "sc");

    /** The letters ECMA reads after a backslash as other than the letter. */
    private static final String ECMA_LETTER_ESCAPES = "dDwWsSbBtnvfrcxupPk";

    static Translation of(String ecma) {
        StringBuilder out = new StringBuilder(ecma.length() + 8);
        boolean inClass = false;
        int i = 0;
        int n = ecma.length();
        while (i < n) {
            char c = ecma.charAt(i);
            if (c == '\\') {
                if (i + 1 >= n) {
                    return why("it ends in a backslash");
                }
                char e = ecma.charAt(i + 1);
                int[] used = {2};
                String written = escape(ecma, i, e, inClass, used);
                if (written == null) {
                    return why(reason(ecma, i, e));
                }
                out.append(written);
                i += used[0];
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                } else if (c == '[') {
                    out.append('\\');
                }
                out.append(c);
                i++;
                continue;
            }
            switch (c) {
                case '[' -> {
                    if (ecma.startsWith("[^]", i)) {
                        out.append("[\\x{0}-\\x{10FFFF}]");
                        i += 3;
                        continue;
                    }
                    if (ecma.startsWith("[]", i)) {
                        out.append("[^\\x{0}-\\x{10FFFF}]");
                        i += 2;
                        continue;
                    }
                    inClass = true;
                    out.append('[');
                    i++;
                    if (i < n && ecma.charAt(i) == '^') {
                        out.append('^');
                        i++;
                    }
                    continue;
                }
                case '(' -> {
                    if (ecma.startsWith("(?=", i) || ecma.startsWith("(?!", i)) {
                        return why("it looks ahead (" + ecma.substring(i, i + 3) + "), which RE2 cannot");
                    }
                    if (ecma.startsWith("(?<=", i) || ecma.startsWith("(?<!", i)) {
                        return why("it looks behind (" + ecma.substring(i, i + 4) + "), which RE2 cannot");
                    }
                    if (ecma.startsWith("(?<", i)) {
                        // a named group: a group (the name is extraction's, and a test extracts nothing)
                        int close = ecma.indexOf('>', i);
                        if (close < 0) {
                            return why("a group name is not closed");
                        }
                        out.append("(?:");
                        i = close + 1;
                        continue;
                    }
                    out.append('(');
                    i++;
                    continue;
                }
                case '{' -> {
                    int close = ecma.indexOf('}', i);
                    String body = close < 0 ? "" : ecma.substring(i + 1, close);
                    if (isRepetition(body)) {
                        int comma = body.indexOf(',');
                        for (String bound : comma < 0 ? new String[] {body}
                                : new String[] {body.substring(0, comma), body.substring(comma + 1)}) {
                            if (!bound.isEmpty() && (bound.length() > 4 || Integer.parseInt(bound) > MAX_REPEAT)) {
                                return why("it repeats " + bound + " times, past RE2's " + MAX_REPEAT);
                            }
                        }
                        out.append(ecma, i, close + 1);
                        i = close + 1;
                    } else {
                        // a { that begins no repetition is the character (ECMA's Annex B)
                        out.append("\\{");
                        i++;
                    }
                    continue;
                }
                case '}' -> {
                    out.append("\\}");
                    i++;
                    continue;
                }
                default -> {
                    out.append(c);
                    i++;
                    continue;
                }
            }
        }
        if (inClass) {
            return why("a character class is not closed");
        }
        return new Translation(out.toString(), null);
    }

    /** Why an escape has no RE2 form. */
    private static String reason(String ecma, int at, char e) {
        if (e >= '1' && e <= '9') {
            return "it refers back to a group (\\" + e + "), which RE2 cannot";
        }
        if (e == 'k') {
            return "it refers back to a named group (\\k), which RE2 cannot";
        }
        if (e == 'p' || e == 'P') {
            int close = ecma.indexOf('}', at);
            return "it names a Unicode property RE2 does not (" + ecma.substring(at, close < 0 ? ecma.length()
                    : close + 1) + ")";
        }
        return "its escape " + ecma.substring(at, Math.min(ecma.length(), at + 6)) + " has no RE2 form";
    }

    /**
     * The escape at {@code at} ({@code \e...}) in RE2, its length set in {@code used}; null when RE2 has no form for
     * it.
     */
    private static @com.legend.base.Nullable String escape(String ecma, int at, char e, boolean inClass, int[] used) {
        switch (e) {
            case 'd', 'D', 'w', 'W', 's', 'S', 't', 'n', 'v', 'f', 'r' -> {
                return "\\" + e;
            }
            case 'b', 'B' -> {
                // in a class ECMA's \b is a backspace; outside it a word boundary, as RE2's
                return inClass ? (e == 'b' ? "\\x08" : null) : "\\" + e;
            }
            case '0' -> {
                return ecma.length() > at + 2 && isDigit(ecma.charAt(at + 2)) ? null : character(0);
            }
            case 'c' -> {
                if (ecma.length() > at + 2 && isAsciiLetter(ecma.charAt(at + 2))) {
                    used[0] = 3;
                    return character(ecma.charAt(at + 2) % 32);
                }
                // \c not followed by a letter: the two characters (Annex B)
                return "\\\\c";
            }
            case 'x' -> {
                if (ecma.length() >= at + 4 && isHex(ecma.charAt(at + 2)) && isHex(ecma.charAt(at + 3))) {
                    used[0] = 4;
                    return character(Integer.parseInt(ecma.substring(at + 2, at + 4), 16));
                }
                return "x";
            }
            case 'u' -> {
                return unicode(ecma, at, used);
            }
            case 'p', 'P' -> {
                return property(ecma, at, e, used);
            }
            case 'k' -> {
                return null;
            }
            default -> {
                if (e >= '1' && e <= '9') {
                    return null;
                }
                if (isAsciiLetter(e) && ECMA_LETTER_ESCAPES.indexOf(e) < 0) {
                    // an identity escape: the letter (RE2 reads \A, \z, \Q as more)
                    return String.valueOf(e);
                }
                // an escaped symbol, or a character beyond ASCII: itself, escaped where RE2 escapes it
                return isAsciiLetterOrDigit(e) || e > 0x7F ? String.valueOf(e) : "\\" + e;
            }
        }
    }

    /** {@code \}{@code uXXXX}, a surrogate pair of them, or {@code \}{@code u{X...}}: the code point in RE2. */
    private static @com.legend.base.Nullable String unicode(String ecma, int at, int[] used) {
        if (ecma.length() > at + 2 && ecma.charAt(at + 2) == '{') {
            int close = ecma.indexOf('}', at);
            String digits = close < 0 ? "" : ecma.substring(at + 3, close);
            if (digits.isEmpty() || digits.length() > 6 || !allHex(digits)) {
                return null;
            }
            int cp = Integer.parseInt(digits, 16);
            if (cp > Character.MAX_CODE_POINT) {
                return null;
            }
            used[0] = close - at + 1;
            return character(cp);
        }
        if (ecma.length() < at + 6 || !allHex(ecma.substring(at + 2, at + 6))) {
            // \ u not followed by four digits: the letter (Annex B)
            return "u";
        }
        int high = Integer.parseInt(ecma.substring(at + 2, at + 6), 16);
        used[0] = 6;
        if (high >= 0xD800 && high <= 0xDBFF && ecma.startsWith("\\u", at + 6) && ecma.length() >= at + 12
                && allHex(ecma.substring(at + 8, at + 12))) {
            int low = Integer.parseInt(ecma.substring(at + 8, at + 12), 16);
            if (low >= 0xDC00 && low <= 0xDFFF) {
                used[0] = 12;
                return character(Character.toCodePoint((char) high, (char) low));
            }
        }
        return character(high);
    }

    /** {@code \p{...}} or {@code \P{...}}: a general category or a script, as RE2 names it. */
    private static @com.legend.base.Nullable String property(String ecma, int at, char e, int[] used) {
        if (ecma.length() <= at + 2 || ecma.charAt(at + 2) != '{') {
            return null;
        }
        int close = ecma.indexOf('}', at);
        if (close < 0) {
            return null;
        }
        String name = ecma.substring(at + 3, close);
        int eq = name.indexOf('=');
        if (eq >= 0) {
            if (!PROPERTY_KEYS.contains(name.substring(0, eq))) {
                return null;
            }
            name = name.substring(eq + 1);
        }
        String spelled = CATEGORIES.getOrDefault(name, name);
        if (!RE2_CATEGORIES.contains(spelled) && !RE2_SCRIPTS.contains(spelled)) {
            return null;
        }
        used[0] = close - at + 1;
        return "\\" + e + "{" + spelled + "}";
    }

    private static boolean isRepetition(String body) {
        int comma = body.indexOf(',');
        String low = comma < 0 ? body : body.substring(0, comma);
        String high = comma < 0 ? "" : body.substring(comma + 1);
        return !low.isEmpty() && allDigits(low) && (high.isEmpty() || allDigits(high)) && body.indexOf(',', comma + 1)
                < 0;
    }

    private static Translation why(String reason) {
        return new Translation(null, reason);
    }

    /** A code point as a pattern spells it on every database: a letter, a digit or a character beyond ASCII as
     *  itself, a symbol escaped, a control character by its code ({@code \x0A}; RE2's {@code \x{...}} past 0xFF). */
    private static String character(int codePoint) {
        if (codePoint > 0x7F && codePoint != 0x85 && !(codePoint >= 0x80 && codePoint <= 0x9F)
                && Character.isDefined(codePoint) && !Character.isISOControl(codePoint)
                && Character.getType(codePoint) != Character.FORMAT
                && Character.getType(codePoint) != Character.SURROGATE) {
            return new String(Character.toChars(codePoint));
        }
        if (codePoint < 0x80 && isAsciiLetterOrDigit((char) codePoint)) {
            return String.valueOf((char) codePoint);
        }
        if (codePoint > 0x20 && codePoint < 0x7F) {
            return "\\" + (char) codePoint;
        }
        String hex = Integer.toHexString(codePoint).toUpperCase(Locale.ROOT);
        return codePoint <= 0xFF ? "\\x" + (hex.length() == 1 ? "0" + hex : hex) : "\\x{" + hex + "}";
    }

    private static boolean allHex(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!isHex(s.charAt(i))) {
                return false;
            }
        }
        return !s.isEmpty();
    }

    private static boolean allDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!isDigit(s.charAt(i))) {
                return false;
            }
        }
        return !s.isEmpty();
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isHex(char c) {
        return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean isAsciiLetterOrDigit(char c) {
        return isAsciiLetter(c) || isDigit(c);
    }
}
