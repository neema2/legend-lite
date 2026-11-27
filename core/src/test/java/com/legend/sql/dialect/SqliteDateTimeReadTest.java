// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DatePart;
import com.legend.sql.DeclaredKind;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DATE-TIME READ OR COMPUTED AT RUN, ON SQLITE (the independent audit of 2026-11-26, findings 2 to 4): every value
 * here is a ROW's -- a column of a one-row source, never a constant the dialect could spell when the plan is made --
 * so the statement's own SQL is what answers, asked of SQLite itself.
 * <ul>
 *   <li>A TEXT read as a date-time ({@code SQLite.instantOf}: a parse, a cast of a text, a Variant's string) is the
 *       one text of its instant ({@code SQLite.timestampLit}) or is REFUSED by name: never some other instant, and
 *       never a text that is none.</li>
 *   <li>A CONSTANT text and a column holding the same text answer the same -- the same one text, or both the refusal
 *       -- shape by shape: the dialect spells a constant at planning only where the statement would read the
 *       same.</li>
 *   <li>Every source that COMPUTES a date-time at run -- adjust and timeBucket by whole seconds, a date built of its
 *       parts, a date at a time, an epoch's milliseconds, now, a bound parameter -- writes that one text.</li>
 * </ul>
 */
class SqliteDateTimeReadTest {

    private static final String URL = "jdbc:sqlite::memory:";

    private static final String REFUSED = "refused";

    /** One cell of a row: a column's name, the type the dialect is told it holds, and its value as written. */
    private record Cell(String name, SqlType type, SqlExpr value) {
        static Cell text(String name, SqlType type, String text) {
            return new Cell(name, type, text == null ? new SqlExpr.NullLit() : new SqlExpr.StringLit(text));
        }
    }

    /** A source of ONE ROW (a VALUES list: the renderer keeps it a source, where a one-row subselect of constants
     *  would be folded into the query and its column read as the constant), its cells held as written: a text is the
     *  text, no cast reads it. */
    private static SqlSource row(Cell... cells) {
        List<SqlExpr> values = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<OutputCol> outputs = new ArrayList<>();
        for (Cell cell : cells) {
            values.add(cell.value());
            names.add(cell.name());
            outputs.add(new OutputCol(cell.name(), cell.type(), true, OutputCol.Origin.DERIVED));
        }
        return new SqlSource.Values(List.of(values), names, "c", outputs);
    }

    private static SqlSource row(String text) {
        return row(Cell.text("v", SqlType.Scalar.VARCHAR, text));
    }

    /** The row's column {@code name}, typed {@code type}: what the dialect sees is a column, never its value. */
    private static SqlExpr column(String name, SqlType type) {
        return SqlExpr.Column.of("c", name, type, true, OutputCol.Origin.DERIVED);
    }

    private static String sql(SqlSource from, SqlExpr value) {
        return new SQLite().render(SqlSelect.starOf(from).withProjections(List.of(
                new SqlSelect.Projection(value, "x", null))));
    }

    /** The statement's one value as text ({@code NULL} for none), or {@link #REFUSED} where it raised one of its own
     *  sites (any other error is the test's failure). */
    private static String answer(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            String v = rs.getString(1);
            return v == null ? "NULL" : v;
        } catch (SQLException raised) {
            if (String.valueOf(raised.getMessage()).contains("LG#1")) {
                return REFUSED;
            }
            throw new SQLException(sql, raised);
        }
    }

    /** The three readings of a text as a date-time: Pure's parseDate, and a cast to each date-time type. */
    private enum Reading {
        PARSE, TIMESTAMP, ZONED;

        SqlExpr of(SqlExpr text) {
            return switch (this) {
                case PARSE -> SqlExpr.Call.of(SqlFn.PARSE_DATE, text);
                case TIMESTAMP -> new SqlExpr.Cast(text, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.USER);
                case ZONED -> new SqlExpr.Cast(text, SqlType.Scalar.TIMESTAMPTZ, SqlExpr.Reason.USER);
            };
        }
    }

    /** {@code text} read at RUN: a column holding it. */
    private static String atRun(Statement st, Reading reading, String text) throws SQLException {
        return answer(st, sql(row(text), reading.of(column("v", SqlType.Scalar.VARCHAR))));
    }

    /** {@code text} read as a CONSTANT the program wrote. */
    private static String constant(Statement st, Reading reading, String text) throws SQLException {
        return answer(st, sql(new SqlSource.Dual(), reading.of(new SqlExpr.StringLit(text))));
    }

    /** The texts that ARE a date or date-time, each with the one text of its instant (a cast's answer; a parse keeps a
     *  day alone a day). */
    private static final Map<String, String> READ = new LinkedHashMap<>();

    static {
        // a day, its fields padded or not
        READ.put("2014-01-01", "2014-01-01 00:00:00");
        READ.put("2014-1-1", "2014-01-01 00:00:00");
        READ.put("14-2-27", "0014-02-27 00:00:00");
        READ.put("0000-01-01", "0000-01-01 00:00:00");
        READ.put("2012-02-29", "2012-02-29 00:00:00");
        // a time: an hour alone, to the minute, to the second, with a 'T' or a space, spaces around it
        READ.put("2014-01-01T10", "2014-01-01 10:00:00");
        READ.put("2014-01-01 10:20", "2014-01-01 10:20:00");
        READ.put("2014-01-01T10:20:30", "2014-01-01 10:20:30");
        READ.put("  2014-01-01T10:20:30  ", "2014-01-01 10:20:30");
        READ.put("2014-01-01  10:20:30", "2014-01-01 10:20:30");
        READ.put("2014-1-1 23:59:59", "2014-01-01 23:59:59");
        // an hour of one digit, as legend-pure's own test writes one (dateDiff.pure, testDateDiffWithDifferentTimeZones)
        READ.put("2015-1-1T5:00:00.000+0100", "2015-01-01 04:00:00");
        READ.put("2014-12-31T23:00:00.000-0500", "2015-01-01 04:00:00");
        READ.put("2014-01-01 5:07", "2014-01-01 05:07:00");
        // a fraction: its digits, no trailing zero; every digit of a longer one kept
        READ.put("2014-01-01 10:20:30.5", "2014-01-01 10:20:30.5");
        READ.put("2014-01-01 10:20:30.500", "2014-01-01 10:20:30.5");
        READ.put("2014-01-01 10:20:30.000", "2014-01-01 10:20:30");
        READ.put("2014-01-01 10:20:00.0", "2014-01-01 10:20:00");
        READ.put("2014-01-01T10:20:30.123456789", "2014-01-01 10:20:30.123456789");
        READ.put("2014-01-01T10:20:30.1234567890", "2014-01-01 10:20:30.123456789");
        READ.put("2014-01-01T10:20:30.1234567891", "2014-01-01 10:20:30.1234567891");
        READ.put("2014-01-01 23:59:59.9999", "2014-01-01 23:59:59.9999");
        // an offset from UTC, applied: Z, +HH:MM, +HHMM, a space before it; across a day, across 1970
        READ.put("2014-01-01T10:20:30Z", "2014-01-01 10:20:30");
        READ.put("2014-01-01T10:20:30z", "2014-01-01 10:20:30");
        READ.put("2014-01-01T10:20Z", "2014-01-01 10:20:00");
        READ.put("2014-01-01 10:20:30.5Z", "2014-01-01 10:20:30.5");
        READ.put("2014-01-01T10:20:30+01:00", "2014-01-01 09:20:30");
        READ.put("2014-01-01T10:20:30.500+0100", "2014-01-01 09:20:30.5");
        READ.put("2014-01-01T10:20:30-0530", "2014-01-01 15:50:30");
        READ.put("2014-01-01T10:20:30+14:00", "2013-12-31 20:20:30");
        READ.put("2014-01-01T10:20+01:00", "2014-01-01 09:20:00");
        READ.put("2014-01-01 00:30:00+01:00", "2013-12-31 23:30:00");
        READ.put("1970-01-01T00:30:00.25+01:00", "1969-12-31 23:30:00.25");
        READ.put("1970-01-01T00:00:00-00:00", "1970-01-01 00:00:00");
        READ.put("2014-01-01 10:20:30.5 +01:00", "2014-01-01 09:20:30.5");
        READ.put("2014-01-01 10:20:30 -0530", "2014-01-01 15:50:30");
        // before 1970, and the ends of the years SQLite's date functions read
        READ.put("1969-07-20 20:17:40.25", "1969-07-20 20:17:40.25");
        READ.put("0001-01-01T00:00:00", "0001-01-01 00:00:00");
        READ.put("9999-12-31T23:59:59.999999999", "9999-12-31 23:59:59.999999999");
    }

    /** The texts that are NO date-time this dialect reads: each refused by name by every reading, never read as some
     *  instant. Until 2026-11-26 the statement kept such a text's day and eight characters of its time. */
    private static final List<String> UNREADABLE = List.of(
            // a time with something after it
            "2014-01-01 10:20:30xyz", "2014-01-01 10:20:30.5junk", "2014-01-01 10:20:30.5.6", "2014-01-01 10:20:30.",
            "2014-01-01 ab", "2014-01-01x", "2014-01-01 10:", "2014-01-01 5", "2014-01-01 1:2:3",
            // an offset SQLite's date functions do not apply: an hour alone, past fourteen hours, no real minutes,
            // after an hour alone
            "2014-01-01 10:20:30+01", "2014-01-01 10:20:30+15:00", "2014-01-01T10:20:30+23:59",
            "2014-01-01 10:20:30+0960", "2014-01-01 10:20:30-ab12", "2014-01-01T10Z", "2014-01-01T10+01:00",
            // an offset that moves the instant out of the years 0000 to 9999
            "0000-01-01T00:30:00+01:00", "9999-12-31T23:30:00-01:00",
            // a day that is none (SQLite's own reading rolls February 30 into March), a time that is none
            "2014-02-30T10:00:00", "2014-02-30", "2013-02-29", "2014-13-01", "2014-00-10", "2014-01-00",
            "2014-01-01 24:00:00", "2014-01-01 10:20:60", "2014-01-01 25:00", "2014-01-01 10:60",
            // no date at all (a bare number is a Julian day to SQLite's date functions, 'now' the clock)
            "", "abc", "2014", "now", "20140101", "1388571630", "2014-01", "2014--1", "-1-01",
            // a day whose fields are not digits, or are more than three; another separator; a tab
            "2014-1x-01", "2014-1-1-5", "2014-01-01t10:20:30", "2014/01/01", "\t2014-01-01");

    /** A year outside 0000 to 9999: a CAST reads one written as this dialect writes it (the year an integer with no
     *  plus and no leading zero, month and day padded, no offset), each with its one text; a parse refuses one, as it
     *  always has. */
    private static final Map<String, String> WIDE = new LinkedHashMap<>();

    static {
        WIDE.put("10000-01-01", "10000-01-01 00:00:00");
        WIDE.put("10000-1-1", "10000-01-01 00:00:00");
        WIDE.put("10000-01-01 10:20:30.50", "10000-01-01 10:20:30.5");
        WIDE.put("25489-03-04T10:30:00", "25489-03-04 10:30:00");
        WIDE.put("25489-03-04T10", "25489-03-04 10:00:00");
        WIDE.put("-21457-01-01 00:00:00.5", "-21457-01-01 00:00:00.5");
        WIDE.put("-1-12-31 23:30:00", "-1-12-31 23:30:00");
        // February 29 by the year's own leap rule: 12000 is a leap year, 10001 is not
        WIDE.put("12000-02-29 10:00", "12000-02-29 10:00:00");
        WIDE.put("10001-02-29", REFUSED);
        WIDE.put("-4-02-29", "-4-02-29 00:00:00");
        WIDE.put("-1-02-29", REFUSED);
        // not as this dialect writes one, a day or time that is none, an offset (none can be applied)
        WIDE.put("+10000-01-01 00:00:00", REFUSED);
        WIDE.put("-0001-01-01", REFUSED);
        WIDE.put("10000-13-01", REFUSED);
        WIDE.put("10000-01-01 24:00:00", REFUSED);
        WIDE.put("10000-01-01 10:20:30xyz", REFUSED);
        WIDE.put("10000-01-01 10:20:30Z", REFUSED);
        WIDE.put("10000-01-01 10:20:30+01:00", REFUSED);
    }

    @Test
    void aTextReadAtRunIsTheOneTextOfItsInstant() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            for (Map.Entry<String, String> e : READ.entrySet()) {
                String instant = e.getValue();
                assertEquals(instant, atRun(st, Reading.TIMESTAMP, e.getKey()), "cast: '" + e.getKey() + "'");
                assertEquals(instant, atRun(st, Reading.ZONED, e.getKey()), "zoned cast: '" + e.getKey() + "'");
                // a parse keeps a day alone a day: the cast around it decides what it is
                String parsed = instant.endsWith(" 00:00:00") && !e.getKey().contains(":")
                        ? instant.substring(0, instant.length() - 9) : instant;
                assertEquals(parsed, atRun(st, Reading.PARSE, e.getKey()), "parse: '" + e.getKey() + "'");
            }
            // no text is a NULL date-time
            assertEquals("NULL", atRun(st, Reading.TIMESTAMP, null));
            assertEquals("NULL", atRun(st, Reading.ZONED, null));
        }
    }

    @Test
    void anUnreadableTextIsRefusedByNameNeverReadAsSomeInstant() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            for (String text : UNREADABLE) {
                for (Reading reading : Reading.values()) {
                    assertEquals(REFUSED, atRun(st, reading, text), reading + ": '" + text + "'");
                }
            }
        }
    }

    @Test
    void aYearOutsideSqlitesIsReadByACastOnlyAsThisDialectWritesOne() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            for (Map.Entry<String, String> e : WIDE.entrySet()) {
                assertEquals(e.getValue(), atRun(st, Reading.TIMESTAMP, e.getKey()), "cast: '" + e.getKey() + "'");
                assertEquals(e.getValue(), atRun(st, Reading.ZONED, e.getKey()), "zoned cast: '" + e.getKey() + "'");
                assertEquals(REFUSED, atRun(st, Reading.PARSE, e.getKey()), "parse: '" + e.getKey() + "'");
            }
        }
    }

    /** THE TWO PATHS, ONE ANSWER (the audit's finding 3): a constant the program wrote may be spelled when the plan
     *  is made ({@code SQLite.constantInstant}) -- over every text above, and every reading, it answers what a column
     *  holding the same text answers: the same one text, or both the refusal. */
    @Test
    void aConstantAndAColumnOfTheSameTextAnswerTheSame() throws SQLException {
        List<String> texts = new ArrayList<>(READ.keySet());
        texts.addAll(UNREADABLE);
        texts.addAll(WIDE.keySet());
        int spelled = 0;
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            for (String text : texts) {
                for (Reading reading : Reading.values()) {
                    assertEquals(atRun(st, reading, text), constant(st, reading, text), reading + ": '" + text + "'");
                    // (how many the dialect spelled: a statement with no reading in it)
                    spelled += sql(new SqlSource.Dual(), reading.of(new SqlExpr.StringLit(text))).contains("datetime(")
                            ? 0 : 1;
                }
            }
        }
        // the comparison is of two different paths: many constants are spelled at planning, and many are not
        assertTrue(spelled > 60 && spelled < 3 * texts.size() - 60, "spelled at planning: " + spelled);
    }

    private static SqlSource json(String json) {
        return row(Cell.text("j", SqlType.Scalar.JSON, json));
    }

    /** A Variant's string read as a date-time: the same read. */
    @Test
    void aVariantsStringIsReadAsATextIs() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            SqlExpr variant = column("j", SqlType.Scalar.JSON);
            SqlExpr read = new SqlExpr.Cast(variant, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.USER);
            assertEquals("2014-01-01 10:20:30.5", answer(st, sql(json("\"2014-01-01T10:20:30.500\""), read)));
            assertEquals("2014-01-01 09:20:30", answer(st, sql(json("\"2014-01-01T10:20:30+01:00\""), read)));
            assertEquals("1969-07-20 20:17:40.25", answer(st, sql(json("\"1969-07-20 20:17:40.250\""), read)));
            assertEquals(REFUSED, answer(st, sql(json("\"2014-02-30T10:00:00\""), read)));
            assertEquals(REFUSED, answer(st, sql(json("5"), read)));
            assertEquals("NULL", answer(st, sql(json("null"), read)));
        }
    }

    /** Whether the database judge reads {@code typed} (a date-time column's cell, as held) and {@code text} as one
     *  instant. */
    private static String judgedSame(Statement st, String typed, String text) throws SQLException {
        SqlExpr test = SqlExpr.Call.of(SqlFn.EQUAL,
                new SqlExpr.Cast(column("t", SqlType.Scalar.TIMESTAMP), SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE),
                new SqlExpr.Cast(new SqlExpr.StringLit(text), SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE));
        return answer(st, sql(row(Cell.text("t", SqlType.Scalar.TIMESTAMP, typed)), new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(test, new SqlExpr.IntLit(1))), new SqlExpr.IntLit(0), SqlExpr.Reason.JUDGE)));
    }

    /** THE DATABASE JUDGE READS A TYPED CELL IN FULL (the audit's finding 1): a date-time column's cell is not always
     *  this dialect's own text -- a store's own table holds what its writer wrote -- so the judge reads it as it reads
     *  a text, into the one text of its instant, as the host judge decodes it; a day's text (an abstract Date's slot)
     *  is its midnight, and a nonzero tenth digit is refused, never cut. */
    @Test
    void theDatabaseJudgeReadsATypedCellInFull() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            for (String cell : List.of("2014-01-01 10:20:30.5", "2014-01-01 10:20:30.500", "2014-01-01T10:20:30.5",
                    "2014-01-01 10:20:30.500000000")) {
                for (String text : List.of("2014-01-01 10:20:30.5", "2014-01-01T10:20:30.500",
                        "2014-01-01 11:20:30.5+01:00", "2014-01-01 10:20:30.500000000Z")) {
                    assertEquals("1", judgedSame(st, cell, text), "'" + cell + "' against '" + text + "'");
                }
                assertEquals("0", judgedSame(st, cell, "2014-01-01 10:20:30.05"), cell);
                assertEquals("0", judgedSame(st, cell, "2014-01-01 10:20:30"), cell);
            }
            assertEquals("1", judgedSame(st, "2014-01-01 10:20:31.000", "2014-01-01T10:20:31"));
            assertEquals("1", judgedSame(st, "2014-01-10", "2014-01-10 00:00:00.0"));
            assertEquals(REFUSED, judgedSame(st, "2014-01-01 10:20:30.1234567891", "2014-01-01 10:20:30.123456789"));
        }
    }

    // ---- the sources that compute a date-time at run -----------------------------------------------------------------

    private static final SqlExpr T = column("t", SqlType.Scalar.TIMESTAMP);

    private static SqlSource held(String instant) {
        return row(Cell.text("t", SqlType.Scalar.TIMESTAMP, instant));
    }

    private static SqlExpr adjust(DatePart unit, long n) {
        return SqlExpr.Call.of(SqlFn.ADD_INTERVAL, new SqlExpr.DatePartLit(unit), new SqlExpr.IntLit(n), T);
    }

    /** adjust by a whole number of seconds, minutes, hours, days or weeks: the second moves, the fraction's digits are
     *  carried whole (all nine), before 1970 as after, and a day's text is its midnight. */
    @Test
    void adjustByWholeSecondsCarriesTheFraction() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            assertEquals("2014-01-01 10:20:31.5", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    adjust(DatePart.SECOND, 1))));
            assertEquals("2014-01-01 10:20:29", answer(st, sql(held("2014-01-01 10:20:30"),
                    adjust(DatePart.SECOND, -1))));
            assertEquals("2014-01-01 10:21:30.123456789", answer(st, sql(held("2014-01-01 10:20:30.123456789"),
                    adjust(DatePart.MINUTE, 1))));
            assertEquals("1970-01-01 00:30:00.75", answer(st, sql(held("1969-12-31 23:30:00.75"),
                    adjust(DatePart.HOUR, 1))));
            assertEquals("1969-12-30 23:59:59.5", answer(st, sql(held("1970-01-01 00:00:00.5"),
                    adjust(DatePart.SECOND, -86401))));
            assertEquals("2014-01-02 10:20:30.5", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    adjust(DatePart.DAY, 1))));
            assertEquals("2013-12-25 10:20:30.5", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    adjust(DatePart.WEEK, -1))));
            assertEquals("2014-01-10 00:00:01", answer(st, sql(held("2014-01-10"), adjust(DatePart.SECOND, 1))));
            // a millisecond and a microsecond move the fraction: no trailing zero is written
            assertEquals("2014-01-01 10:20:31", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    adjust(DatePart.MILLISECOND, 500))));
            assertEquals("2014-01-01 10:20:30.500001", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    adjust(DatePart.MICROSECOND, 1))));
        }
    }

    private static SqlExpr bucket(DatePart unit, long size) {
        return SqlExpr.Call.of(SqlFn.TIME_BUCKET, new SqlExpr.DatePartLit(unit), new SqlExpr.IntLit(size), T);
    }

    /** timeBucket by whole seconds starts on a whole second, floored before 1970 as after (DuckDB's answers, origin
     *  1970-01-01; a week's origin its Monday). */
    @Test
    void timeBucketByWholeSecondsStartsOnAWholeSecond() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            assertEquals("1969-07-20 19:00:00", answer(st, sql(held("1969-07-20 20:17:40.25"),
                    bucket(DatePart.HOUR, 7))));
            assertEquals("2013-12-31 20:00:00", answer(st, sql(held("2014-01-01 00:00:00"), bucket(DatePart.HOUR, 7))));
            assertEquals("2014-01-01 10:20:30", answer(st, sql(held("2014-01-01 10:20:30.999999999"),
                    bucket(DatePart.SECOND, 1))));
            assertEquals("2014-01-01 10:20:00", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    bucket(DatePart.MINUTE, 5))));
            assertEquals("2014-01-01 00:00:00", answer(st, sql(held("2014-01-01 10:20:30.5"), bucket(DatePart.DAY, 1))));
            assertEquals("2013-12-30 00:00:00", answer(st, sql(held("2014-01-01 10:20:30.5"),
                    bucket(DatePart.WEEK, 1))));
            assertEquals("1969-12-29 00:00:00", answer(st, sql(held("1969-12-31 23:59:59.5"),
                    bucket(DatePart.WEEK, 1))));
        }
    }

    private static String diff(Statement st, DatePart unit, String from, String to) throws SQLException {
        return answer(st, sql(row(Cell.text("t", SqlType.Scalar.TIMESTAMP, from), Cell.text("u", SqlType.Scalar.TIMESTAMP, to)),
                SqlExpr.Call.of(SqlFn.DATE_DIFF, new SqlExpr.DatePartLit(unit), T,
                        column("u", SqlType.Scalar.TIMESTAMP))));
    }

    /** dateDiff counts the unit's boundaries crossed: a fraction never rounds a boundary in, before 1970 as after. */
    @Test
    void dateDiffCountsBoundariesByTheWholeSecond() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            assertEquals("1", diff(st, DatePart.SECOND, "1969-12-31 23:59:58.5", "1969-12-31 23:59:59.4"));
            assertEquals("0", diff(st, DatePart.SECOND, "2014-01-01 10:20:30.1", "2014-01-01 10:20:30.9"));
            assertEquals("-1", diff(st, DatePart.SECOND, "1900-03-01 00:00:00.5", "1900-02-28 23:59:59.5"));
            assertEquals("-1", diff(st, DatePart.MINUTE, "1900-03-01 00:00:00.5", "1900-02-28 23:59:59.5"));
            assertEquals("-1", diff(st, DatePart.HOUR, "1900-03-01 00:00:00.5", "1900-02-28 23:59:59.5"));
            assertEquals("1", diff(st, DatePart.HOUR, "2014-01-01 10:59:59.999", "2014-01-01 11:00:00"));
            assertEquals("24", diff(st, DatePart.HOUR, "2014-01-01", "2014-01-02 00:00:00.5"));
        }
    }

    /** An epoch's milliseconds, a date built of its parts and a date at a time: each the one text, no trailing zero. */
    @Test
    void theBuiltDateTimesAreTheOneText() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL); Statement st = c.createStatement()) {
            SqlExpr ms = SqlExpr.Call.of(SqlFn.FROM_EPOCH_MS, column("n", SqlType.Scalar.BIGINT));
            Map<Long, String> epochs = new LinkedHashMap<>();
            epochs.put(1388571630000L, "2014-01-01 10:20:30");
            epochs.put(1388571630500L, "2014-01-01 10:20:30.5");
            epochs.put(1388571630120L, "2014-01-01 10:20:30.12");
            epochs.put(-14182939750L, "1969-07-20 20:17:40.25");
            epochs.put(-1L, "1969-12-31 23:59:59.999");
            epochs.put(0L, "1970-01-01 00:00:00");
            for (Map.Entry<Long, String> e : epochs.entrySet()) {
                assertEquals(e.getValue(), answer(st, sql(row(new Cell("n", SqlType.Scalar.BIGINT,
                        new SqlExpr.IntLit(e.getKey()))), ms)),
                        "from the milliseconds " + e.getKey());
            }

            List<SqlExpr> parts = new ArrayList<>();
            long[] whole = {2014, 1, 2, 3, 4};
            for (int i = 0; i < whole.length; i++) {
                parts.add(column("p" + i, SqlType.Scalar.BIGINT));
            }
            parts.add(column("s", SqlType.Scalar.DOUBLE));
            SqlExpr made = SqlExpr.Call.of(SqlFn.MAKE_TIMESTAMP, parts.toArray(new SqlExpr[0]));
            Map<Double, String> seconds = new LinkedHashMap<>();
            seconds.put(5.5, "2014-01-02 03:04:05.5");
            seconds.put(5.0, "2014-01-02 03:04:05");
            seconds.put(59.125, "2014-01-02 03:04:59.125");
            for (Map.Entry<Double, String> e : seconds.entrySet()) {
                List<Cell> cells = new ArrayList<>();
                for (int i = 0; i < whole.length; i++) {
                    cells.add(new Cell("p" + i, SqlType.Scalar.BIGINT, new SqlExpr.IntLit(whole[i])));
                }
                cells.add(new Cell("s", SqlType.Scalar.DOUBLE, new SqlExpr.FloatLit(e.getKey())));
                assertEquals(e.getValue(), answer(st, sql(row(cells.toArray(new Cell[0])), made)),
                        "a date of the seconds " + e.getKey());
            }

            SqlExpr at = SqlExpr.Call.of(SqlFn.DATE_AT_TIME, column("d", SqlType.Scalar.DATE),
                    column("tm", SqlType.Scalar.TIME));
            Map<List<String>, String> at2 = new LinkedHashMap<>();
            at2.put(List.of("2014-01-01", "10:20:30.500"), "2014-01-01 10:20:30.5");
            at2.put(List.of("2014-01-01", "10:20:00"), "2014-01-01 10:20:00");
            at2.put(List.of("1969-07-20", "20:17:40.25"), "1969-07-20 20:17:40.25");
            for (Map.Entry<List<String>, String> e : at2.entrySet()) {
                assertEquals(e.getValue(), answer(st, sql(row(Cell.text("d", SqlType.Scalar.DATE, e.getKey().get(0)),
                        Cell.text("tm", SqlType.Scalar.TIME, e.getKey().get(1))), at)), e.getKey().toString());
            }

            // now: the clock's milliseconds, the same one text (no trailing zero, no bare point)
            String now = answer(st, sql(new SqlSource.Dual(), SqlExpr.Call.of(SqlFn.NOW)));
            assertTrue(now.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{0,2}[1-9])?"), now);
        }
    }

    /** {@code bound} sent for every placeholder of the statement of a parameter declared {@code kind}: its value. */
    private static String parameter(Connection c, DeclaredKind kind, String bound) throws SQLException {
        SqlExpr.PlanParam p = new SqlExpr.PlanParam("p", kind == DeclaredKind.DATE_TIME
                ? SqlExpr.PlanParam.ValueType.DATETIME : SqlExpr.PlanParam.ValueType.DATE, false,
                kind == DeclaredKind.DATE_TIME ? new TypeFact.Typed(SqlType.Scalar.TIMESTAMP, true)
                        : com.legend.sql.SqlTyping.UNKNOWN, kind);
        RenderedStatement s = new SQLite().renderStatement(SqlSelect.starOf(new SqlSource.Dual()).withProjections(
                List.of(new SqlSelect.Projection(p, "x", null))));
        try (PreparedStatement ps = c.prepareStatement(s.sql())) {
            for (int i = 1; i <= s.binds().size(); i++) {
                ps.setString(i, bound);
            }
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), s.sql());
                return rs.getString(1);
            }
        }
    }

    /** A bound DateTime or Date parameter: exec binds a date-time's nine digits for every database
     *  ({@code PlanRunner.dateTimeText}) and a day's ten characters; the statement holds the one text. */
    @Test
    void aBoundParameterIsTheOneText() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL)) {
            for (DeclaredKind kind : List.of(DeclaredKind.DATE_TIME, DeclaredKind.DATE)) {
                assertEquals("2014-01-01 10:20:30.5", parameter(c, kind, "2014-01-01 10:20:30.500000000"), kind.name());
                assertEquals("2014-01-01 10:20:30", parameter(c, kind, "2014-01-01 10:20:30.000000000"), kind.name());
                assertEquals("2014-01-01 10:20:00", parameter(c, kind, "2014-01-01 10:20:00.000000000"), kind.name());
                assertEquals("1969-07-20 20:17:40.123456789", parameter(c, kind, "1969-07-20 20:17:40.123456789"),
                        kind.name());
            }
            // a Date parameter may be a day: its ten characters, no zero of its day taken for a fraction's
            assertEquals("2014-01-10", parameter(c, DeclaredKind.DATE, "2014-01-10"));
            assertEquals("2010-10-10", parameter(c, DeclaredKind.DATE, "2010-10-10"));
        }
    }
}
