// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two statements the test-data generator writes through its session's dialect (the dialect program's Phase 3, the PR
 * audit of 2026-10-16): a temporary table from a query ({@link SqlDml.CreateTempTableAs}), spelled where the
 * generator's sessions run it and refused by every other dialect; and a query on one line
 * ({@link SqlDialect#renderInline}), as the engine compares a trail with its newlines removed.
 */
class TempTableAsTest {

    private static SqlSelect values(String... texts) {
        List<SqlSelect.Projection> ps = new java.util.ArrayList<>();
        for (int i = 0; i < texts.length; i++) {
            ps.add(new SqlSelect.Projection(new SqlExpr.StringLit(texts[i]), "v" + i, null));
        }
        return SqlSelect.starOf(new SqlSource.Dual()).withProjections(ps);
    }

    @Test
    void theGeneratorsSessionsHoldAQuerysRowsInATemporaryTable() throws Exception {
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new H2(), "jdbc:h2:mem:temp_as"),
                Map.entry(new H2Modern(), "jdbc:h2:mem:temp_as_modern"), Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new SQLite(), "jdbc:sqlite::memory:"))) {
            SqlDialect d = db.getKey();
            String name = d.getClass().getSimpleName();
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                String create = d.render(new SqlDml.CreateTempTableAs("tdg_t", values("a")));
                assertTrue(create.startsWith("CREATE TEMPORARY TABLE tdg_t AS "), name + ": " + create);
                st.execute(create);
                try (ResultSet rs = st.executeQuery("SELECT * FROM tdg_t")) {
                    assertTrue(rs.next(), name);
                    assertEquals("a", rs.getString(1), name);
                    assertFalse(rs.next(), name);
                }
            }
        }
    }

    /** Postgres spells it too (the writes design's leg 6, 2026-10-19: a Cypher statement's staged rows, its run measured
     *  on the embedded Postgres 16 by //core:cypher_parity_postgres_test), the table's name as a query reads a bare
     *  name: folded (PR #67's review, 2026-10-22: {@code "legend_stage_rowsA"} was made, {@code legend_stage_rowsa}
     *  read). */
    @Test
    void postgresSpellsItToo() {
        String create = new Postgres().render(new SqlDml.CreateTempTableAs("tdg_t", values("a")));
        assertEquals("CREATE TEMPORARY TABLE tdg_t AS SELECT 'a' AS \"v0\";", create);
        String staged = new Postgres().render(SqlDml.CreateTempTableAs.staged("legend_stage_rowsA", values("a"),
                List.of("v0")));
        assertEquals("CREATE TEMPORARY TABLE legend_stage_rowsa (\"v0\") AS SELECT 'a' AS \"v0\";", staged);
        assertEquals("legend_stage_rowsa", new Postgres().tableName("legend_stage_rowsA",
                com.legend.sql.NameQuoting.BARE), "the name a query reads");
    }

    /** A program's staged rows on H2 are its transaction's (PR #67's review, 2026-10-22, measured on H2 2.1.214):
     *  made without committing what the program wrote before them, seen by no other session, and gone when the
     *  transaction ends, committed or rolled back, with no DROP (which would commit). */
    @Test
    void h2StagesRowsInTheProgramsTransaction() throws Exception {
        for (H2 d : List.of(new H2(), new H2Modern())) {
            String name = d.getClass().getSimpleName();
            assertTrue(d.dropsStagedTablesWithTheirTransaction(), name);
            String url = "jdbc:h2:mem:staged_" + name;
            try (Connection c = DriverManager.getConnection(url); Connection other = DriverManager.getConnection(url);
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER)");
                c.setAutoCommit(false);
                st.execute("INSERT INTO T VALUES (1)");
                String create = d.render(SqlDml.CreateTempTableAs.staged("legend_stage_s", values("a"),
                        List.of("v0")));
                assertTrue(create.startsWith("CREATE LOCAL TEMPORARY TABLE legend_stage_s ON COMMIT DROP"
                        + " TRANSACTIONAL AS "), name + ": " + create);
                st.execute(create);
                try (ResultSet rs = st.executeQuery("SELECT * FROM legend_stage_s")) {
                    assertTrue(rs.next(), name);
                }
                try (Statement o = other.createStatement(); ResultSet rs = o.executeQuery("SELECT count(*) FROM T")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), name + ": the staged table committed the program's write");
                }
                assertThrows(java.sql.SQLException.class,
                        () -> other.createStatement().executeQuery("SELECT * FROM legend_stage_s"), name);
                c.rollback();
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM T")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), name);
                }
                assertThrows(java.sql.SQLException.class, () -> st.executeQuery("SELECT * FROM legend_stage_s"),
                        name + ": the staged table outlived its transaction");
            }
        }
    }

    @Test
    void everyOtherDialectRefusesIt() {
        for (SqlDialect d : List.<SqlDialect>of(new MariaDb(), new MySql(), new Trino(),
                new ClickHouse(), new Snowflake(), new Databricks(), new BigQuery(), new EngineStyleH2(),
                new EngineStyleDB2(), new EngineStyleComposite())) {
            DialectCapability refusal = assertThrows(DialectCapability.class,
                    () -> d.render(new SqlDml.CreateTempTableAs("tdg_t", values("a"))), d.getClass().getSimpleName());
            assertTrue(refusal.getMessage().contains(d.getClass().getSimpleName()), refusal.getMessage());
        }
    }

    /** One line, a literal's own newline kept: only the layout's newlines go. */
    @Test
    void aQueryOnOneLine() {
        SqlSelect inner = values("a\nb", "c");
        SqlSelect outer = SqlSelect.starOf(new SqlSource.Subselect(inner, "s", null));
        for (SqlDialect d : List.<SqlDialect>of(new DuckDb(), new H2(), new SQLite(), new Postgres())) {
            String name = d.getClass().getSimpleName();
            String laidOut = d.render(outer);
            String inline = d.renderInline(outer);
            assertTrue(laidOut.replace("'a\nb'", "").contains("\n"), name + " lays a subquery out: " + laidOut);
            assertTrue(inline.contains("'a\nb'"), name + ": " + inline);
            assertFalse(inline.replace("'a\nb'", "").contains("\n"), name + ": " + inline);
            // and the next render lays out again
            assertEquals(laidOut, d.render(outer), name);
        }
    }
}
