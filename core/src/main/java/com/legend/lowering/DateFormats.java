// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.DateFmt;

import java.util.ArrayList;
import java.util.List;

/** Date-format pattern translation for the scalar lowering rules. */
final class DateFormats {

    private DateFormats() {
    }

    /**
     * Java SimpleDateFormat pattern -> TYPED parts, longest token first
     * (remediation T3.2: dialects spell parts; nobody re-parses). Values
     * are UTC throughout, so the ZONE directives are literals: Z prints the
     * +0000 offset, X the ISO 'Z'.
     */
    static List<DateFmt> javaDateToParts(String pattern) {
        List<DateFmt> out = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        int i = 0;
        while (i < pattern.length()) {
            if (pattern.charAt(i) == '"') {
                // quoted text, read as legend-pure reads it (DateFormatPattern.parseQuotedText, legend-pure 5.99.0;
                // C5-J item 5, 2026-11-15: until then the next quote closed it and a backslash was text): inside the
                // quotes a backslash makes text of the character after it, the quote and the backslash included
                int j = i + 1;
                boolean closed = false;
                while (j < pattern.length()) {
                    char c = pattern.charAt(j++);
                    if (c == '\\' && j < pattern.length()) {
                        text.append(pattern.charAt(j++));
                    } else if (c == '"') {
                        closed = true;
                        break;
                    } else {
                        text.append(c);
                    }
                }
                if (!closed) {
                    // pure's pattern parser's words for it. Through %t a quote left open usually swallows the brace
                    // that would end the pattern, which endOfPattern refuses first, as pure's scan does; but a
                    // zone-shifted pattern reaches this (the C5-J audit's nit, 2026-11-15): Scalars.dateWithPattern reads
                    // the zone up to its first ']', blind to quotes, so in %t{["EST]yyyy"} the text after the zone
                    // opens a quote it never closes (pure reads the zone by its own rule; start-here's C5-J backlog)
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.LOWER,
                            "Missing closing quote in format string: " + pattern);
                }
                i = j;
                continue;
            }
            String rest = pattern.substring(i);
            Object[][] tokens = {
                    {"yyyy", DateFmt.Part.YEAR4}, {"SSS", DateFmt.Part.SUBSEC_MIN},
                    {"MM", DateFmt.Part.MONTH2}, {"dd", DateFmt.Part.DAY2},
                    {"HH", DateFmt.Part.HOUR2}, {"hh", DateFmt.Part.HOUR12},
                    {"mm", DateFmt.Part.MIN2}, {"ss", DateFmt.Part.SEC2},
                    {"h", DateFmt.Part.HOUR12_NOPAD}, {"a", DateFmt.Part.AMPM},
            };
            String[][] literals = {{"Z", "+0000"}, {"X", "Z"}};
            boolean matched = false;
            for (Object[] t : tokens) {
                if (rest.startsWith((String) t[0])) {
                    if (text.length() > 0) {
                        out.add(new DateFmt.Text(text.toString()));
                        text.setLength(0);
                    }
                    out.add((DateFmt.Part) t[1]);
                    i += ((String) t[0]).length();
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                for (String[] t : literals) {
                    if (rest.startsWith(t[0])) {
                        text.append(t[1]);
                        i += t[0].length();
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                char ch = pattern.charAt(i);
                // A pattern LETTER outside the token table would silently
                // pass through as literal text ('MMM' -> '03M'; audit) —
                // loud instead; punctuation/separators pass.
                // (a token not implemented: refused by name, the C4 audit's M3, 2026-11-13; until then a bare
                // IllegalStateException)
                if (Character.isLetter(ch)) {
                    throw new com.legend.error.NotImplementedException("unsupported date-format"
                            + " token '" + ch + "' in pattern '" + pattern + "'");
                }
                text.append(ch);
                i++;
            }
        }
        if (text.length() > 0) {
            out.add(new DateFmt.Text(text.toString()));
        }
        return out;
    }

    /**
     * The closing brace of the date pattern a {@code %t} opens, its brace at {@code open} in {@code format}, found as
     * legend-pure finds it ({@code FormatTools.findEndOfDateFormatString}, legend-pure 5.99.0; C5-J item 5, 2026-11-15:
     * until then the first brace, quotes unread): a double quote opens a run of text and the next one closes it, inside
     * such a run a backslash makes text of the character after it, and the first closing brace outside quoted text ends
     * the pattern. A pattern that never ends -- no closing brace, or one inside an unclosed quote -- is refused at
     * planning with pure's words, which pure says when it compiles a literal format string ({@code FormatValidator}):
     * register S342.
     */
    static int endOfPattern(String format, int open) {
        boolean inQuotes = false;
        boolean escaped = false;
        for (int i = open + 1; i < format.length(); i++) {
            char c = format.charAt(i);
            if (escaped) {
                escaped = false;
            } else if (inQuotes) {
                if (c == '"') {
                    inQuotes = false;
                } else if (c == '\\') {
                    escaped = true;
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == '}') {
                return i;
            }
        }
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.LOWER,
                "Could not find end of date format starting at index " + open + " of: " + format);
    }

    /** The places {@code letter} stands in {@code pattern} outside its quoted text, quotes read as
     *  {@link #javaDateToParts} reads them (C5-J item 5, 2026-11-15: a quoted {@code Z} is text, as in pure). */
    static List<Integer> unquoted(String pattern, char letter) {
        List<Integer> at = new ArrayList<>();
        boolean inQuotes = false;
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escaped) {
                escaped = false;
            } else if (inQuotes) {
                if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inQuotes = false;
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == letter) {
                at.add(i);
            }
        }
        return at;
    }

    /**
     * Longest-first token map: engine format spellings (SimpleDateFormat +
     * the Oracle-style TO_CHAR tokens the corpus mixes in) to TYPED parts
     * (until 2026-10-15 through DuckDB's strftime codes, the dialect
     * program's Phase 2). Case is load-bearing (MM month vs mm minutes). Two
     * entries are pinned by ENGINE-PRODUCED expected values rather than a
     * spec: '.mmm' and '.FF' both read as fractional seconds — the
     * sqlFunction corpus asserts %2016-06-23T15:03:00.000 for
     * 'yyyy-MM-dd hh:mm:ss.mmm' over '2016-06-23 15:03:00.000000000', i.e.
     * the engine result treats the tail as millis, not minutes
     * (SimpleDateFormat's reading would shift the minute field). SSS is
     * SimpleDateFormat millis proper.
     */
    private static final Object[][] FORMAT_TOKENS = {
            {"HH24", DateFmt.Part.HOUR2}, {"SSS", DateFmt.Part.SUBSEC_MIN}, {"MMM", DateFmt.Part.MONTH_ABBREV},
            {"MON", DateFmt.Part.MONTH_ABBREV}, {"mmm", DateFmt.Part.SUBSEC_MIN},
            {"yyyy", DateFmt.Part.YEAR4}, {"YYYY", DateFmt.Part.YEAR4}, {"MM", DateFmt.Part.MONTH2},
            {"dd", DateFmt.Part.DAY2}, {"DD", DateFmt.Part.DAY2}, {"MI", DateFmt.Part.MIN2},
            {"mm", DateFmt.Part.MIN2}, {"hh", DateFmt.Part.HOUR2}, {"HH", DateFmt.Part.HOUR2},
            {"ss", DateFmt.Part.SEC2}, {"SS", DateFmt.Part.SEC2}, {"FF", DateFmt.Part.SUBSEC_MIN},
    };

    /** Pure's format vocabulary parses ONCE, here, into TYPED parts —
     * no dialect ever re-parses a format string (remediation T3.2). */
    static List<DateFmt> pureToParts(String src) {
        List<DateFmt> out = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        int i = 0;
        outer:
        while (i < src.length()) {
            for (Object[] tok : FORMAT_TOKENS) {
                if (src.startsWith((String) tok[0], i)) {
                    if (text.length() > 0) {
                        out.add(new DateFmt.Text(text.toString()));
                        text.setLength(0);
                    }
                    out.add((DateFmt.Part) tok[1]);
                    i += ((String) tok[0]).length();
                    continue outer;
                }
            }
            text.append(src.charAt(i));
            i++;
        }
        if (text.length() > 0) {
            out.add(new DateFmt.Text(text.toString()));
        }
        return out;
    }
}
