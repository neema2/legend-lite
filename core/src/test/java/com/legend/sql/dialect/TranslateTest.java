// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.error.DialectCapability;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlFn#TRANSLATE}, the SQL islands' ASCII-only case folding (the tenth line's ask, answered 2026-10-15):
 * every dialect answers it, with its own spelling or a refusal by name, and the in-process databases are asked what it
 * means. Each character of {@code from} becomes the character at its position in {@code to}; one past {@code to}'s
 * end is removed; a character outside {@code from}, a non-ASCII one included, is kept; a NULL string is NULL.
 */
class TranslateTest {

    private static SqlSelect translate(SqlExpr str, String from, String to) {
        return SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(
                SqlExpr.Call.of(SqlFn.TRANSLATE, str, new SqlExpr.StringLit(from), new SqlExpr.StringLit(to)), "v",
                null)));
    }

    private static SqlSelect translate(String str, String from, String to) {
        return translate(new SqlExpr.StringLit(str), from, to);
    }

    /** Each dialect's spelling: the database's own function where it has one. */
    @Test
    void eachDatabaseWithATranslateSpellsItsOwn() {
        Map<SqlDialect, String> spelled = Map.ofEntries(
                Map.entry(new DuckDb(), "translate("),
                Map.entry(new Postgres(), "translate("),
                Map.entry(new H2(), "translate("),
                Map.entry(new Trino(), "translate("),
                Map.entry(new Snowflake(), "translate("),
                Map.entry(new Databricks(), "translate("),
                Map.entry(new BigQuery(), "TRANSLATE("),
                // translate maps ASCII only; translateUTF8 any character
                Map.entry(new ClickHouse(), "translateUTF8("));
        spelled.forEach((dialect, call) -> {
            String sql = dialect.render(translate("aBc", "ABC", "abc"));
            assertTrue(sql.contains(call + "'aBc', 'ABC', 'abc')"), dialect.getClass().getSimpleName() + ": " + sql);
        });
    }

    /** A database with no translate refuses it by name: none is emulated, so no answer is a guess. */
    @Test
    void aDatabaseWithoutOneRefusesItByName() {
        Map<SqlDialect, String> refused = Map.of(
                new SQLite(), "SQLite",
                new MariaDb(), "MariaDB",
                new MySql(), "MySQL",
                new EngineStyleH2(), "engine text",
                new EngineStyleDB2(), "engine text",
                new EngineStyleComposite(), "engine text");
        refused.forEach((dialect, named) -> {
            DialectCapability refusal = assertThrows(DialectCapability.class,
                    () -> dialect.render(translate("aBc", "ABC", "abc")), dialect.getClass().getSimpleName());
            assertTrue(refusal.getMessage().contains(named), refusal.getMessage());
        });
    }

    /** What it means, asked of DuckDB and H2 (in process): mapped in one pass, the first of a repeated character
     *  winning, a character past {@code to}'s end removed (DuckDB; H2 refuses that case, below), the rest kept. */
    @Test
    void theInProcessDatabasesMeanTheSame() throws Exception {
        for (var db : List.of(Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new H2(), "jdbc:h2:mem:translate"))) {
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                String name = db.getKey().getClass().getSimpleName();
                assertEquals("abcÄx", one(st, db.getKey().render(translate("aBcÄx", "ABC", "abc"))), name);
                // one pass: a's b is not mapped again to c
                assertEquals("bc", one(st, db.getKey().render(translate("ab", "ab", "bc"))), name);
                assertEquals("xxb", one(st, db.getKey().render(translate("aab", "aa", "xy"))), name);
                assertEquals("xbc", one(st, db.getKey().render(translate("abc", "a", "xyz"))), name);
                assertNull(one(st, db.getKey().render(translate(new SqlExpr.NullLit(), "a", "b"))), name);
            }
        }
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            assertEquals("xx", one(st, new DuckDb().render(translate("abcabc", "abc", "x"))));
        }
    }

    /** H2's TRANSLATE keeps a character past {@code to}'s end ({@code xbcxbc}, measured), so a shorter {@code to}
     *  is refused, not spelled: H2's answer would be another function's. */
    @Test
    void h2RefusesAShorterTo() {
        for (SqlDialect h2 : List.of(new H2())) {
            DialectCapability refusal = assertThrows(DialectCapability.class,
                    () -> h2.render(translate("abcabc", "abc", "x")));
            assertTrue(refusal.getMessage().contains("H2"), refusal.getMessage());
        }
    }

    private static Object one(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getObject(1);
        }
    }
}
