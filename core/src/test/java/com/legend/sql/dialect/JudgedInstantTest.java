// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The judge's instant of a date-time on each database the platform runs in-process (register S399; S181 and the user's
 * rule of 2026-11-22, "do nanoseconds typed for whatever range allows, refuse if out of range"): an assert whose values
 * carry nanoseconds is read to the nanosecond ({@code JUDGE_NANOS}), typed, refused by name outside the days the
 * database's nanosecond type holds; any other to the microsecond ({@code JUDGE}), every year; a text with a nonzero
 * digit past the ones the reading holds is refused, never cut or rounded.
 */
class JudgedInstantTest {

    private static final String NINE = "2020-01-01 10:00:00.123456789";

    private static SqlExpr text(String t) {
        return new SqlExpr.StringLit(t);
    }

    /** Whether the two values are one instant under {@code dialect}'s judge reading {@code reason}, asked of its
     *  database. */
    private static boolean same(AnsiSqlRenderer dialect, String url, SqlExpr.Reason reason, SqlExpr e, SqlExpr a)
            throws SQLException {
        SqlExpr test = SqlExpr.Call.of(SqlFn.EQUAL, new SqlExpr.Cast(e, SqlType.Scalar.TIMESTAMP, reason),
                new SqlExpr.Cast(a, SqlType.Scalar.TIMESTAMP, reason));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(
                new SqlExpr.Case(List.of(new SqlExpr.Case.When(test, new SqlExpr.IntLit(1))), new SqlExpr.IntLit(0),
                        SqlExpr.Reason.JUDGE), "v", null)));
        String sql = dialect.render(q);
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getInt(1) == 1;
        }
    }

    /** The database's raise: one of the statement's raise sites, which exec reads back and words -- {@code site}, the
     *  first value's (a digit past the ones read is a reading's first site; a day outside the nanosecond type's, the
     *  second of DuckDB's nanosecond reading). */
    private static void refused(AnsiSqlRenderer dialect, String url, SqlExpr.Reason reason, SqlExpr e, SqlExpr a,
            int site) {
        String name = dialect.getClass().getSimpleName();
        SQLException raised = assertThrows(SQLException.class, () -> same(dialect, url, reason, e, a), name);
        assertTrue(String.valueOf(raised.getMessage()).contains("LG#" + site), name + ": " + raised.getMessage());
    }

    /** Nanoseconds compared, typed, every digit -- the reading of an assert whose values carry them. */
    private static void everyDigitCompared(AnsiSqlRenderer dialect, String url) throws SQLException {
        String name = dialect.getClass().getSimpleName();
        SqlExpr.Reason r = SqlExpr.Reason.JUDGE_NANOS;
        assertTrue(same(dialect, url, r, text(NINE), text("2020-01-01T10:00:00.1234567890")), name);
        assertTrue(same(dialect, url, r, text("2020-01-01T10:00:00.5"), text("2020-01-01 10:00:00.500")), name);
        assertFalse(same(dialect, url, r, text(NINE), text("2020-01-01 10:00:00.123456")), name);
        assertFalse(same(dialect, url, r, text(NINE), text("2020-01-01 10:00:00.123456788")), name);
        // a value the database types as a date-time against its text: one instant, every digit
        assertTrue(same(dialect, url, r, new SqlExpr.TimestampLit("2020-01-01T10:00:00.123456789"), text(NINE)), name);
        assertFalse(same(dialect, url, r, new SqlExpr.TimestampLit("2020-01-01T10:00:00.123456789"),
                text("2020-01-01 10:00:00.123456")), name);
        // a digit past the ninth: zeros are no digit; any other is refused by the database, never cut
        assertTrue(same(dialect, url, r, text("2020-01-01 10:00:00.1234567890000"), text(NINE)), name);
        refused(dialect, url, r, text("2020-01-01 10:00:00.1234567891"), text(NINE), 1);
    }

    /** The microsecond reading: every year, a digit past the sixth refused where the database's date-time holds six. */
    private static void everyYearCompared(AnsiSqlRenderer dialect, String url) throws SQLException {
        String name = dialect.getClass().getSimpleName();
        SqlExpr.Reason r = SqlExpr.Reason.JUDGE;
        assertTrue(same(dialect, url, r, text("9999-12-31 23:59:59"), text("9999-12-31T23:59:59.000")), name);
        assertTrue(same(dialect, url, r, text("1624-12-10T04:44:48"), new SqlExpr.TimestampLit("1624-12-10T04:44:48")),
                name);
        assertFalse(same(dialect, url, r, text("2407-05-19T19:15:12"), text("2407-05-19T19:15:13")), name);
    }

    @Test
    void duckDbComparesNanosecondsInItsNanosecondTypeAndRefusesTheDaysItLacks() throws SQLException {
        DuckDb d = new DuckDb();
        everyDigitCompared(d, "jdbc:duckdb:");
        everyYearCompared(d, "jdbc:duckdb:");
        // to the nanosecond, a day outside 1677-09-22 to 2262-04-10 is refused by name, a value DuckDB types and a text
        refused(d, "jdbc:duckdb:", SqlExpr.Reason.JUDGE_NANOS, text("9999-12-31 23:59:59.123456789"), text(NINE), 2);
        // (a value DuckDB types has no digits to read: its day is its reading's first site)
        refused(d, "jdbc:duckdb:", SqlExpr.Reason.JUDGE_NANOS, new SqlExpr.TimestampLit("1600-01-01T00:00:00"),
                text(NINE), 1);
        assertTrue(same(d, "jdbc:duckdb:", SqlExpr.Reason.JUDGE_NANOS, text("2262-04-10 23:59:59.999999999"),
                text("2262-04-10T23:59:59.999999999")));
        // to the microsecond, a text's seventh digit is refused, never cut
        refused(d, "jdbc:duckdb:", SqlExpr.Reason.JUDGE, text(NINE), text("2020-01-01 10:00:00.123456"), 1);
    }

    @Test
    void h2ComparesEveryDigitInEveryYear() throws SQLException {
        H2 h2 = new H2();
        everyDigitCompared(h2, "jdbc:h2:mem:");
        everyYearCompared(h2, "jdbc:h2:mem:");
        assertTrue(same(h2, "jdbc:h2:mem:", SqlExpr.Reason.JUDGE_NANOS, text("9999-12-31 23:59:59.123456789"),
                text("9999-12-31T23:59:59.123456789")));
    }

    @Test
    void sqliteComparesEveryDigitInEveryYear() throws SQLException {
        SQLite sqlite = new SQLite();
        everyDigitCompared(sqlite, "jdbc:sqlite::memory:");
        everyYearCompared(sqlite, "jdbc:sqlite::memory:");
        assertTrue(same(sqlite, "jdbc:sqlite::memory:", SqlExpr.Reason.JUDGE_NANOS,
                text("9999-12-31 23:59:59.123456789"), text("9999-12-31T23:59:59.123456789")));
        // a date-time is held as the one text of its instant (SQLite.timestampLit, 2026-11-26), and a text is read
        // into the same one: a value and a text written with other digits are one instant, in every year
        SqlExpr.Reason r = SqlExpr.Reason.JUDGE;
        assertTrue(same(sqlite, "jdbc:sqlite::memory:", r, new SqlExpr.TimestampLit("2020-01-01T10:00:00.500"),
                text("2020-01-01 10:00:00.5")));
        assertTrue(same(sqlite, "jdbc:sqlite::memory:", r, new SqlExpr.TimestampLit("2020-01-01T10:00:00"),
                text("2020-01-01T10:00:00.000")));
        assertTrue(same(sqlite, "jdbc:sqlite::memory:", r, new SqlExpr.TimestampLit("2020-01-01T10:00:00.000"),
                new SqlExpr.TimestampLit("2020-01-01T10:00:00")));
        assertFalse(same(sqlite, "jdbc:sqlite::memory:", r, new SqlExpr.TimestampLit("2020-01-01T10:00:00.5"),
                text("2020-01-01 10:00:00.05")));
        assertTrue(same(sqlite, "jdbc:sqlite::memory:", r, new SqlExpr.TimestampLit("-21457-01-01T00:00:00.50"),
                text("-21457-01-01 00:00:00.5")));
        assertTrue(same(sqlite, "jdbc:sqlite::memory:", r, new SqlExpr.TimestampLit("25489-03-04T10:30:00"),
                text("25489-03-04T10:30:00.0")));
    }
}
