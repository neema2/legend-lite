// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlToPure.Kind;
import com.legend.protocol.sql.SqlToPure.Tx;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * IQ's dates and timestamps as text, for the {@code #IQ{...}#} island (docs/IQ_SQL_STUDY_2026_10_13.md §9.7; IQ island,
 * 2026-10-14, M1e): the counterpart of the Postgres island's {@code PostgresDateTime}. Every rule below was measured on
 * SQL Anywhere 17 (IQ's front end), in an IQ-like and a default database alike, under each DATE_ORDER.
 *
 * <p><b>Reading</b> ({@link #read}) is of a constant string only, at translation: the island reads a literal's spelling,
 * as it reads a number's, and never a value a query computes (a string that is no constant stays refused by name). The
 * spellings read, each the same in every profile unless it says otherwise:
 * <ul>
 *   <li>a four-digit year first: {@code yyyy-m-d}, {@code yyyy/m/d}, {@code yyyy.m.d} (one or two digits for the month
 *       and the day; either separator at each place); {@code yyyymmdd}; {@code yymmdd} (its two-digit year below);</li>
 *   <li>three numbers of one or two digits (the last also of four), separated by {@code -}, {@code /} or {@code .}, in
 *       the profile's DATE_ORDER: YMD reads {@code 03/04/05} as 2003-04-05 (its last number of four digits is IQ's
 *       error), MDY as 2005-03-04, DMY as 2005-04-03;</li>
 *   <li>a month's English name or its three-letter abbreviation, in any case: {@code Mar 4 2005}, {@code March 4,
 *       2005}, {@code Mar 4 05}, {@code 4 Mar 2005}, {@code 04-Mar-2005}, {@code 2005 Mar 4}, {@code 2005-Mar-04} (a
 *       four-digit year, or a two-digit one right after the month and the day);</li>
 *   <li>a two-digit (or one-digit) year by NEAREST_CENTURY (50 in every profile; IQ island, 2026-10-14, the options
 *       framework: any value from 0 to 100 the settings give it): below it, of the 2000s, else of the
 *       1900s ({@code 49} is 2049, {@code 50} 1950);</li>
 *   <li>then, after spaces or a {@code T}, a time: {@code h}, {@code h:n}, {@code h:n:s} (one or two digits each), a
 *       fraction after {@code .}, {@code :} or {@code ,} read as decimal places ({@code 12:34:56:7} is .7; past six
 *       places cut, not rounded), and {@code AM} or {@code PM} (12 AM is midnight, 12 PM noon, a PM hour past 12 kept);
 *       {@code 24:00:00} is the next day's midnight.</li>
 *   <li>a year alone, or a year and a month ({@code 2010}, {@code 2010-05}, {@code 2010/5}, {@code 2010 May},
 *       {@code May 2010}): the month's first day (IQ island, 2026-10-17, measured on IQ 17.1).</li>
 * </ul>
 * Numbers ending in a four-digit year under DATE_ORDER YMD are IQ's error ({@link #unconvertible}).
 * Anything else is not read and refused by name by the caller: a time alone (IQ fills in the current date; IQ's
 * engine reads it on 1900-01-01, {@link #timeAlone}), two digits alone (a century there), weekday names, ISO week
 * and ordinal dates, a zone offset, a time after a tab or with spaces inside it, a month name's other abbreviations
 * ({@code Sept}; IQ matches any prefix). A date before the year 100 is refused by name (review 3's B3: lite's
 * lowering writes its year without its zeros), and so is a time of day with hours or minutes after the year 7910
 * (review 4's M2: SQL Anywhere 17 drops them there). An invalid date or time (IQ's error -157) is not read either.
 *
 * <p><b>Writing</b> ({@link #text}) a DATE or a TIMESTAMP as text -- CAST to a string type, {@code ||}, STRING,
 * LENGTH -- is composed of Pure's own natives (the parts of the date, each written with its zeros), in the profile's
 * DATE_FORMAT and TIMESTAMP_FORMAT: {@code YYYY-MM-DD} and {@code YYYY-MM-DD HH:NN:SS.SSS} (three places of the
 * second, cut) at the database's defaults and on a TDS login, {@code yyyy-mm-dd hh:nn:ss.ssssss} (six) through IQ's
 * ODBC driver.
 */
final class IqDateTime {

    private IqDateTime() {
    }

    /** The months by their English names and three-letter abbreviations, lower case. */
    private static final Map<String, Integer> MONTHS = months();

    private static Map<String, Integer> months() {
        String[] names = {"january", "february", "march", "april", "may", "june", "july", "august", "september",
            "october", "november", "december"};
        Map<String, Integer> m = new java.util.HashMap<>();
        for (int i = 0; i < names.length; i++) {
            m.put(names[i], i + 1);
            m.put(names[i].substring(0, 3), i + 1);
        }
        return java.util.Collections.unmodifiableMap(m);
    }

    // ------------------------------------------------------------------ reading

    /**
     * {@code text}, a constant string, as IQ reads it as a date or a timestamp under {@code settings} (see the class
     * doc): its date ({@code yyyy-mm-dd}) and its time ({@code hh:nn:ss}, then {@code .ffffff} when the second has a
     * fraction); null when it is no spelling the island reads, or no valid date. A date before the year 100, or a
     * timestamp after 7910 with hours or minutes, is refused by name ({@code pos}).
     */
    static String @com.legend.base.Nullable [] read(String text, IqSettings settings, SqlAst.Pos pos) {
        int[] v = fields(text, settings.dateOrder(), settings.nearestCentury());
        if (v == null) {
            return null;
        }
        int year = v[0];
        int month = v[1];
        int day = v[2];
        if (year < 1 || year > 9999 || month < 1 || month > 12 || day < 1 || day > daysIn(year, month)) {
            return null;
        }
        if (v[3] == 24) {
            // the next day's midnight
            day++;
            if (day > daysIn(year, month)) {
                day = 1;
                month++;
                if (month > 12) {
                    month = 1;
                    year++;
                }
            }
            if (year > 9999) {
                return null;
            }
            v[3] = 0;
        }
        if (year < 100) {
            throw SqlTranslationException.unsupported("a date before the year 100 is not supported yet: lite's lowering"
                    + " writes its year without leading zeros, which Postgres reads as this century's", pos);
        }
        if (year > IqDialect.LAST_TIMED_YEAR && (v[3] != 0 || v[4] != 0)) {
            // IQ island, 2026-10-14 (review 4's M2, measured; moved here by the merge of M1e): SQL Anywhere 17 reads
            // '7911-01-01 10:30' as 7911-01-01 00:00 (its defect, the study's §2.5); its seconds it keeps
            throw SqlTranslationException.unsupported("a timestamp after the year " + IqDialect.LAST_TIMED_YEAR
                    + " with hours or minutes is not supported: SQL Anywhere 17 drops them there (its defect), so no"
                    + " answer of it judges lite's", pos);
        }
        String date = pad(year, 4) + "-" + pad(month, 2) + "-" + pad(day, 2);
        String time = pad(v[3], 2) + ":" + pad(v[4], 2) + ":" + pad(v[5], 2);
        if (v[6] != 0) {
            time += "." + pad(v[6], 6);
        }
        return new String[] {date, time};
    }

    /**
     * IQ island, 2026-10-16 (the corpus lane's sede-36359, measured on IQ 17.1): {@code text}, a constant string, as
     * IQ's engine reads a time alone -- {@code h:n}, {@code h:n:s}, a fraction and AM or PM as after a date (an ISO
     * {@code T} before it read too) -- on 1900-01-01: its time ({@code hh:nn:ss}, then {@code .ffffff}); null when it is
     * no time read here. An hour alone is IQ's error -1001006, and {@code 24:00} (midnight of the same day there, not of
     * the next as after a date) is not read. SQL Anywhere's front end reads a time alone on the current date.
     */
    static @com.legend.base.Nullable String timeAlone(String text) {
        List<String> tokens = tokens(text);
        if (tokens == null || tokens.size() < 3 || !tokens.get(tokens.get(0).equals("T") ? 2 : 1).equals(":")) {
            return null;
        }
        List<String> t = new ArrayList<>(tokens);
        if (!t.get(0).equals("T")) {
            t.add(0, " ");
        }
        int[] v = {0, 0, 0, 0};
        if (!time(t, v) || v[0] == 24) {
            return null;
        }
        String time = pad(v[0], 2) + ":" + pad(v[1], 2) + ":" + pad(v[2], 2);
        return v[3] == 0 ? time : time + "." + pad(v[3], 6);
    }

    /** The fields {@code text} spells -- year, month, day, hour (24 for the next day's midnight), minute, second and
     *  microseconds -- or null for a spelling not read or a time out of range. */
    private static int @com.legend.base.Nullable [] fields(String text, String order, int nearest) {
        List<String> tokens = tokens(text);
        if (tokens == null || tokens.isEmpty()) {
            return null;
        }
        int[] date = new int[3];
        int used = date(tokens, order, nearest, date);
        if (used < 0) {
            return null;
        }
        int[] time = {0, 0, 0, 0};
        if (used < tokens.size() && !time(tokens.subList(used, tokens.size()), time)) {
            return null;
        }
        return new int[] {date[0], date[1], date[2], time[0], time[1], time[2], time[3]};
    }

    /**
     * The text cut into tokens: runs of digits, runs of letters, and each other character (a space between two
     * tokens dropped, one before the time kept as a token {@code " "} -- the date's and the time's boundary). Leading and
     * trailing spaces are dropped; null for a character no spelling read here holds (a tab, a sign, ...).
     */
    private static @com.legend.base.Nullable List<String> tokens(String text) {
        int from = 0;
        int to = text.length();
        while (from < to && text.charAt(from) == ' ') {
            from++;
        }
        while (to > from && text.charAt(to - 1) == ' ') {
            to--;
        }
        String s = text.substring(from, to);
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            int j = i + 1;
            if (digit(c)) {
                while (j < s.length() && digit(s.charAt(j))) {
                    j++;
                }
            } else if (letter(c)) {
                while (j < s.length() && letter(s.charAt(j))) {
                    j++;
                }
            } else if (c == ' ') {
                while (j < s.length() && s.charAt(j) == ' ') {
                    j++;
                }
            } else if ("-/.:,".indexOf(c) < 0) {
                return null;
            }
            out.add(c == ' ' ? " " : s.substring(i, j));
            i = j;
        }
        return out;
    }

    /** Reads the date at the start of {@code t} into {@code out} (year, month, day): the tokens it takes, or -1. */
    private static int date(List<String> t, String order, int nearest, int[] out) {
        String first = t.get(0);
        if (digits(first)) {
            if (first.length() == 8 || first.length() == 6) {
                // yyyymmdd, yymmdd
                int y = first.length() == 8 ? Integer.parseInt(first.substring(0, 4))
                        : century(Integer.parseInt(first.substring(0, 2)), nearest);
                int at = first.length() - 4;
                out[0] = y;
                out[1] = Integer.parseInt(first.substring(at, at + 2));
                out[2] = Integer.parseInt(first.substring(at + 2));
                return 1;
            }
            if (t.size() >= 5 && separator(t.get(1)) && separator(t.get(3)) && digits(t.get(2))
                    && digits(t.get(4)) && small(t.get(2))) {
                if (first.length() == 4) {
                    if (!small(t.get(4))) {
                        return -1;
                    }
                    out[0] = Integer.parseInt(first);
                    out[1] = Integer.parseInt(t.get(2));
                    out[2] = Integer.parseInt(t.get(4));
                    return 5;
                }
                if (!small(first) || !t.get(1).equals(t.get(3))) {
                    return -1;
                }
                return byOrder(first, t.get(2), t.get(4), order, nearest, out) ? 5 : -1;
            }
            if (t.size() >= 5 && first.length() == 4 && dash(t.get(1)) && letters(t.get(2)) && dash(t.get(3))
                    && digits(t.get(4)) && small(t.get(4))) {
                // yyyy-Mon-d, yyyy Mon d
                Integer m = MONTHS.get(t.get(2).toLowerCase(java.util.Locale.ROOT));
                return m == null ? -1 : set(out, Integer.parseInt(first), m, Integer.parseInt(t.get(4)), 5);
            }
            if (t.size() >= 5 && small(first) && dash(t.get(1)) && letters(t.get(2)) && dash(t.get(3))
                    && digits(t.get(4)) && t.get(4).length() == 4) {
                // d Mon yyyy, d-Mon-yyyy
                Integer m = MONTHS.get(t.get(2).toLowerCase(java.util.Locale.ROOT));
                return m == null ? -1 : set(out, Integer.parseInt(t.get(4)), m, Integer.parseInt(first), 5);
            }
            // IQ island, 2026-10-17 (the corpus lane, measured on IQ 17.1 in both engines): a year alone, or a year
            // and a month, is read on the month's first day -- '2010' is 2010-01-01, '2010-05', '2010/5', '2010.05'
            // and '2010 May' are 2010-05-01 -- a time after it read as after a date. (Two digits alone are read as a
            // century there, '20' 2000-01-01 but '00' IQ's error: not read.)
            if (first.length() == 4 && t.size() >= 3 && separator(t.get(1)) && digits(t.get(2)) && small(t.get(2))
                    && boundary(t, 3)) {
                return set(out, Integer.parseInt(first), Integer.parseInt(t.get(2)), 1, 3);
            }
            if (first.length() == 4 && t.size() >= 3 && t.get(1).equals(" ") && letters(t.get(2))
                    && !t.get(2).equals("T") && boundary(t, 3)) {
                Integer m = MONTHS.get(t.get(2).toLowerCase(java.util.Locale.ROOT));
                return m == null ? -1 : set(out, Integer.parseInt(first), m, 1, 3);
            }
            if (first.length() == 4 && boundary(t, 1)) {
                return set(out, Integer.parseInt(first), 1, 1, 1);
            }
            return -1;
        }
        if (letters(first) && t.size() >= 3 && t.get(1).equals(" ") && digits(t.get(2)) && t.get(2).length() == 4
                && boundary(t, 3)) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): Mon yyyy, the month's first day
            Integer m = MONTHS.get(first.toLowerCase(java.util.Locale.ROOT));
            return m == null ? -1 : set(out, Integer.parseInt(t.get(2)), m, 1, 3);
        }
        if (letters(first) && t.size() >= 5 && t.get(1).equals(" ") && digits(t.get(2)) && small(t.get(2))) {
            // Mon d yyyy, Mon d, yyyy, Mon d yy
            Integer m = MONTHS.get(first.toLowerCase(java.util.Locale.ROOT));
            int at = 3;
            if (t.get(at).equals(",")) {
                at++;
                if (at < t.size() && t.get(at).equals(" ")) {
                    at++;
                }
            } else if (t.get(at).equals(" ")) {
                at++;
            } else {
                return -1;
            }
            if (m == null || at >= t.size() || !digits(t.get(at))
                    || t.get(at).length() != 4 && t.get(at).length() != 2) {
                return -1;
            }
            String y = t.get(at);
            return set(out, y.length() == 4 ? Integer.parseInt(y) : century(Integer.parseInt(y), nearest), m,
                    Integer.parseInt(t.get(2)), at + 1);
        }
        return -1;
    }

    /** Whether the date read so far ends at token {@code at}: the text's end, or the space or ISO {@code T} before a
     *  time. */
    private static boolean boundary(List<String> t, int at) {
        return at == t.size() || t.get(at).equals(" ") || t.get(at).equals("T");
    }

    /**
     * IQ island, 2026-10-17 (the corpus lane's SEDE, measured on IQ 17.1): whether {@code text} is a date IQ cannot
     * convert under {@code settings} -- under DATE_ORDER YMD, numbers ending in a four-digit year ({@code 01/02/2018},
     * {@code 8-16-2011}, {@code 05/2010}, with a time after it or not): IQ's error wherever it is converted (-157 in
     * SQL Anywhere's front end; under IQ's engine -1009145 compared, -1001006 cast). Other spellings not read stay
     * refused by name.
     */
    static boolean unconvertible(String text, IqSettings settings) {
        if (!settings.dateOrder().equalsIgnoreCase("YMD")) {
            return false;
        }
        List<String> t = tokens(text);
        if (t == null || t.size() < 3 || !small(t.get(0)) || !separator(t.get(1)) || !digits(t.get(2))) {
            return false;
        }
        if (t.get(2).length() == 4) {
            return boundary(t, 3);
        }
        return small(t.get(2)) && t.size() >= 5 && separator(t.get(3)) && digits(t.get(4)) && t.get(4).length() == 4
                && boundary(t, 5);
    }

    /** Three numbers of a date in the order DATE_ORDER says, a year of one or two digits by NEAREST_CENTURY. */
    private static boolean byOrder(String a, String b, String c, String order, int nearest, int[] out) {
        boolean yearFirst = order.equalsIgnoreCase("YMD");
        String year = yearFirst ? a : c;
        if (yearFirst ? !small(c) : !small(c) && c.length() != 4) {
            return false;
        }
        int y = year.length() == 4 ? Integer.parseInt(year) : century(Integer.parseInt(year), nearest);
        int first = Integer.parseInt(yearFirst ? b : a);
        int second = Integer.parseInt(yearFirst ? c : b);
        boolean monthFirst = yearFirst || order.equalsIgnoreCase("MDY");
        if (!monthFirst && !order.equalsIgnoreCase("DMY")) {
            return false;
        }
        set(out, y, monthFirst ? first : second, monthFirst ? second : first, 0);
        return true;
    }

    private static int set(int[] out, int year, int month, int day, int used) {
        out[0] = year;
        out[1] = month;
        out[2] = day;
        return used;
    }

    /** Reads the time after a date ({@code t} starts at the boundary): hour, minute, second, microseconds; false when
     *  it is no time read here or one out of range. */
    private static boolean time(List<String> t, int[] out) {
        // spaces, or ISO's T
        if (!t.get(0).equals(" ") && !t.get(0).equals("T")) {
            return false;
        }
        int i = 1;
        int[] parts = {-1, 0, 0};
        int n = 0;
        while (i < t.size() && n < 3) {
            String d = t.get(i);
            if (!digits(d) || !small(d)) {
                return false;
            }
            parts[n++] = Integer.parseInt(d);
            i++;
            if (i < t.size() && t.get(i).equals(":") && n < 3 && i + 1 < t.size() && digits(t.get(i + 1))) {
                i++;
                continue;
            }
            break;
        }
        if (n == 0) {
            return false;
        }
        int micros = 0;
        if (n == 3 && i < t.size() && (t.get(i).equals(".") || t.get(i).equals(":") || t.get(i).equals(","))) {
            i++;
            if (i < t.size() && digits(t.get(i))) {
                String f = (t.get(i) + "000000").substring(0, 6);
                micros = Integer.parseInt(f);
                i++;
            }
        }
        String meridiem = null;
        if (i < t.size() && t.get(i).equals(" ")) {
            i++;
            if (i >= t.size()) {
                return false;
            }
        }
        if (i < t.size()) {
            String m = t.get(i).toLowerCase(java.util.Locale.ROOT);
            if (!m.equals("am") && !m.equals("pm")) {
                return false;
            }
            meridiem = m;
            i++;
        }
        if (i != t.size()) {
            return false;
        }
        int hour = parts[0];
        int minute = parts[1];
        int second = parts[2];
        if (minute > 59 || second > 59) {
            return false;
        }
        if (meridiem != null) {
            if (meridiem.equals("am")) {
                if (hour > 12) {
                    return false;
                }
                hour = hour == 12 ? 0 : hour;
            } else {
                if (hour == 0 || hour > 23) {
                    return false;
                }
                hour = hour < 12 ? hour + 12 : hour;
            }
        } else if (hour == 24) {
            if (minute != 0 || second != 0 || micros != 0) {
                return false;
            }
        } else if (hour > 23) {
            return false;
        }
        out[0] = hour;
        out[1] = minute;
        out[2] = second;
        out[3] = micros;
        return true;
    }

    /** A two-digit (or one-digit) year by NEAREST_CENTURY ({@code nearest}). */
    private static int century(int yy, int nearest) {
        return yy < nearest ? 2000 + yy : 1900 + yy;
    }

    /** The days of {@code month} in {@code year}, by the Gregorian calendar. */
    static int daysIn(int year, int month) {
        return switch (month) {
            case 2 -> year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 29 : 28;
            case 4, 6, 9, 11 -> 30;
            case 1, 3, 5, 7, 8, 10, 12 -> 31;
            default -> throw new IllegalArgumentException("no month " + month);
        };
    }

    private static boolean separator(String s) {
        return s.equals("-") || s.equals("/") || s.equals(".");
    }

    /** A month name's separator: a space, {@code -} or {@code /}. */
    private static boolean dash(String s) {
        return s.equals(" ") || s.equals("-") || s.equals("/");
    }

    /** One or two digits. */
    private static boolean small(String s) {
        return digits(s) && s.length() <= 2;
    }

    private static boolean digits(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!digit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean letters(String s) {
        return !s.isEmpty() && letter(s.charAt(0));
    }

    private static boolean digit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean letter(char c) {
        return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z';
    }

    private static String pad(int v, int width) {
        StringBuilder b = new StringBuilder(Integer.toString(v));
        while (b.length() < width) {
            b.insert(0, '0');
        }
        return b.toString();
    }

    // ------------------------------------------------------------------ writing

    /** The places of a second a TIMESTAMP written as text shows under {@code settings}'s TIMESTAMP_FORMAT: 3 (cut) at
     *  the defaults and on a TDS login, 6 through IQ's ODBC driver (measured); -1 for a format not read. */
    static int places(IqSettings settings) {
        String f = settings.timestampFormat().toUpperCase(java.util.Locale.ROOT);
        return f.equals("YYYY-MM-DD HH:NN:SS.SSS") ? 3 : f.equals("YYYY-MM-DD HH:NN:SS.SSSSSS") ? 6 : -1;
    }

    /** The length of a DATE's (or a TIMESTAMP's) text under {@code settings}: 10 (or 20 and the second's places). */
    static int length(boolean timestamp, IqSettings settings) {
        return timestamp ? 20 + places(settings) : 10;
    }

    /**
     * {@code x}, a DATE or a TIMESTAMP, as text in {@code settings}'s DATE_FORMAT or TIMESTAMP_FORMAT (a VARCHAR; NULL
     * for NULL): each part of the date written with its zeros ({@code lpad} of its digits), a timestamp's second cut to
     * the format's places -- composed of Pure's natives, so the database computes it for each value. A format the
     * island does not read is refused by name ({@code what} names the conversion, {@code pos} its place).
     */
    static Tx text(Tx x, IqSettings settings, String what, SqlAst.Pos pos) {
        boolean timestamp = x.kind() == Kind.DATETIME;
        if (!settings.dateFormat().equalsIgnoreCase("YYYY-MM-DD") || timestamp && places(settings) < 0) {
            throw SqlTranslationException.unsupported(what + " of a " + (timestamp ? "timestamp" : "date") + " to text"
                    + " under " + (timestamp ? "TIMESTAMP_FORMAT " + settings.timestampFormat() : "DATE_FORMAT "
                    + settings.dateFormat()) + " is not supported yet", pos);
        }
        int places = places(settings);
        Tx r = SqlToPure.strict(Kind.STRING, List.of(x), v -> {
            ValueSpecification d = v.get(0);
            List<ValueSpecification> parts = new ArrayList<>(List.of(digits(SqlToPure.call("year", d), 4),
                    new CString("-"), digits(SqlToPure.call("monthNumber", d), 2), new CString("-"),
                    digits(SqlToPure.call("dayOfMonth", d), 2)));
            if (timestamp) {
                parts.addAll(List.of(new CString(" "), digits(SqlToPure.call("hour", d), 2), new CString(":"),
                        digits(SqlToPure.call("minute", d), 2), new CString(":"),
                        digits(SqlToPure.call("second", d), 2), new CString("."),
                        digits(fraction(d, places), places)));
            }
            return com.legend.protocol.spec.AppliedFunction.infixRun("plus", parts);
        });
        return Tx.value(r.vs(), Kind.STRING, r.nullable(), new IqType("VARCHAR"));
    }

    /** {@code n}'s digits, zeros before them to {@code width}. */
    private static ValueSpecification digits(ValueSpecification n, int width) {
        return SqlToPure.call("lpad", SqlToPure.call("toString", n), new CInteger((long) width), new CString("0"));
    }

    /** The fraction of {@code d}'s second at {@code places} places (3 or 6), cut: its microseconds of the second (a
     *  remainder of the epoch's, which a timestamp before 1970 makes negative, kept in 0 .. 999999), divided down. */
    private static ValueSpecification fraction(ValueSpecification d, int places) {
        ValueSpecification epoch = SqlToPure.call("toEpochValue", d, new EnumValue(
                "meta::pure::functions::date::DurationUnit", "MICROSECONDS"));
        CInteger million = new CInteger(1_000_000L);
        ValueSpecification micros = SqlToPure.call("mod", com.legend.protocol.spec.AppliedFunction.infixRun("plus",
                List.of(SqlToPure.call("mod", epoch, million), million)), million);
        return places == 6 ? micros : SqlToPure.call("floor", SqlToPure.call("divide", micros, new CInteger(1000L)));
    }
}
