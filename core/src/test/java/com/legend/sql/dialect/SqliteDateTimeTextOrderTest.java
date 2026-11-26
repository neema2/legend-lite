// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE ONE TEXT OF A DATE-TIME, AND ITS ORDER (the owner's ruling of 2026-11-26; {@code SQLite.timestampLit}'s note):
 * SQLite has no date-time type, so its dialect holds a date-time as text, and the product's {@code =}, {@code <},
 * {@code IN}, {@code DISTINCT}, {@code GROUP BY}, joins and sorts compare that text. Two properties make the text's
 * comparison the instant's, each asked of SQLite itself:
 * <ol>
 *   <li>ONE TEXT: every way of writing one instant -- a literal with no fraction, with trailing zeros, with nine
 *       digits; a text read as a date-time with a space or a {@code T}, with {@code Z}, with an offset from UTC -- is
 *       held as the same text;</li>
 *   <li>ITS ORDER: over every pair of instants, exactly one of {@code <}, {@code =}, {@code >} holds between their
 *       texts, and it is the one that holds between the instants -- a whole second against the same second with a
 *       fraction, {@code .5} against {@code .25} and {@code .05}, the last nanosecond of a minute against the next
 *       minute's first second, a day's end against the next day, the years 0001 and 9999, a time before 1970.</li>
 * </ol>
 * Not held here, by the dialect's own note: a year outside 0000 to 9999 (one text still, but no order across year
 * widths) and a store's own column in a table the dialect did not load (compared as it is held).
 */
class SqliteDateTimeTextOrderTest {

    private static final String URL = "jdbc:sqlite::memory:";

    private static final List<LocalDateTime> INSTANTS = List.of(
            LocalDateTime.of(1, 1, 1, 0, 0, 0),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000),
            LocalDateTime.of(1970, 1, 1, 0, 0, 0),
            LocalDateTime.of(2014, 1, 1, 0, 0, 0),
            LocalDateTime.of(2014, 1, 1, 0, 0, 0, 1),
            LocalDateTime.of(2014, 1, 1, 0, 0, 0, 50_000_000),
            LocalDateTime.of(2014, 1, 1, 0, 0, 0, 250_000_000),
            LocalDateTime.of(2014, 1, 1, 0, 0, 0, 500_000_000),
            LocalDateTime.of(2014, 1, 1, 0, 0, 0, 999_999_999),
            LocalDateTime.of(2014, 1, 1, 0, 0, 1),
            LocalDateTime.of(2014, 1, 1, 0, 0, 9, 500_000_000),
            LocalDateTime.of(2014, 1, 1, 0, 0, 10),
            LocalDateTime.of(2014, 1, 1, 0, 0, 59),
            LocalDateTime.of(2014, 1, 1, 0, 0, 59, 999_999_999),
            LocalDateTime.of(2014, 1, 1, 0, 1, 0),
            LocalDateTime.of(2014, 1, 1, 9, 59, 59, 123_456_000),
            LocalDateTime.of(2014, 1, 1, 10, 0, 0),
            LocalDateTime.of(2014, 1, 1, 23, 59, 59, 900_000_000),
            LocalDateTime.of(2014, 1, 2, 0, 0, 0),
            LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999));

    /** {@code t}'s day and second, {@code separator} between them. */
    private static String second(LocalDateTime t, char separator) {
        return String.format(java.util.Locale.ROOT, "%04d-%02d-%02d%c%02d:%02d:%02d", t.getYear(), t.getMonthValue(),
                t.getDayOfMonth(), separator, t.getHour(), t.getMinute(), t.getSecond());
    }

    /** The ways {@code t}'s fraction is written: its digits with no trailing zero, with one more zero, all nine; and,
     *  where it is zero, none at all. */
    private static Set<String> fractions(LocalDateTime t) {
        String nine = String.format(java.util.Locale.ROOT, "%09d", t.getNano());
        String shortest = nine.replaceFirst("0+$", "");
        Set<String> out = new LinkedHashSet<>();
        if (shortest.isEmpty()) {
            out.add("");
            out.add(".0");
        } else {
            out.add("." + shortest);
            if (shortest.length() < 9) {
                out.add("." + shortest + "0");
            }
        }
        out.add("." + nine);
        return out;
    }

    /** Every way this test writes {@code t}: a literal of each fraction, and a text read as a date-time -- with a
     *  space and with a 'T', bare, with {@code Z}, with {@code +00:00} and {@code +0000}, and (away from the first and
     *  the last day lite's years hold) the same instant as the wall time an hour east. */
    private static List<SqlExpr> writings(LocalDateTime t) {
        List<SqlExpr> out = new ArrayList<>();
        for (String f : fractions(t)) {
            out.add(new SqlExpr.TimestampLit(second(t, 'T') + f));
            for (char separator : new char[] {' ', 'T'}) {
                for (String zone : List.of("", "Z", "+00:00", "+0000")) {
                    out.add(read(second(t, separator) + f + zone));
                }
            }
            if (t.getYear() > 1 && t.getYear() < 9999) {
                out.add(read(second(t.plusHours(1), ' ') + f + "+01:00"));
                out.add(read(second(t.minusHours(5).minusMinutes(30), 'T') + f + "-0530"));
                out.add(new SqlExpr.TimestampLit(second(t.plusHours(1), 'T') + f + "+01:00"));
            }
        }
        return out;
    }

    private static SqlExpr read(String text) {
        return new SqlExpr.Cast(new SqlExpr.StringLit(text), SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.USER);
    }

    private static String sql(SqlExpr... values) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            ps.add(new SqlSelect.Projection(values[i], "v" + i, null));
        }
        return new SQLite().render(SqlSelect.starOf(new SqlSource.Dual()).withProjections(ps));
    }

    /** The text SQLite holds for each instant, every writing of it held to that one text. */
    private static Map<LocalDateTime, String> held(Statement st) throws SQLException {
        Map<LocalDateTime, String> held = new LinkedHashMap<>();
        for (LocalDateTime t : INSTANTS) {
            for (SqlExpr writing : writings(t)) {
                String statement = sql(writing);
                try (ResultSet rs = st.executeQuery(statement)) {
                    rs.next();
                    String text = rs.getString(1);
                    String first = held.putIfAbsent(t, text);
                    assertEquals(first == null ? text : first, text, t + " written as " + statement);
                }
            }
        }
        return held;
    }

    @Test
    void everyWritingOfAnInstantIsOneText() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            Map<LocalDateTime, String> held = held(st);
            // and the text is the one the note says: its second, then its fraction's digits with no trailing zero
            for (Map.Entry<LocalDateTime, String> e : held.entrySet()) {
                String fraction = String.format(java.util.Locale.ROOT, "%09d", e.getKey().getNano())
                        .replaceFirst("0+$", "");
                assertEquals(second(e.getKey(), ' ') + (fraction.isEmpty() ? "" : "." + fraction), e.getValue());
            }
            assertEquals(INSTANTS.size(), new LinkedHashSet<>(held.values()).size(), "two instants, one text");
        }
    }

    @Test
    void theTextsCompareAsTheirInstantsDo() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            for (LocalDateTime a : INSTANTS) {
                for (LocalDateTime b : INSTANTS) {
                    // each side a literal written with its nine digits: the dialect holds the one text, and the
                    // database compares the two
                    SqlExpr x = new SqlExpr.TimestampLit(second(a, 'T') + String.format(java.util.Locale.ROOT, ".%09d",
                            a.getNano()));
                    SqlExpr y = new SqlExpr.TimestampLit(second(b, 'T') + String.format(java.util.Locale.ROOT, ".%09d",
                            b.getNano()));
                    try (ResultSet rs = st.executeQuery(sql(SqlExpr.Call.of(SqlFn.LESS, x, y),
                            SqlExpr.Call.of(SqlFn.EQUAL, x, y), SqlExpr.Call.of(SqlFn.GREATER, x, y)))) {
                        rs.next();
                        int order = a.compareTo(b);
                        assertEquals(List.of(order < 0, order == 0, order > 0),
                                List.of(rs.getInt(1) == 1, rs.getInt(2) == 1, rs.getInt(3) == 1), a + " against " + b);
                    }
                }
            }
        }
    }
}
