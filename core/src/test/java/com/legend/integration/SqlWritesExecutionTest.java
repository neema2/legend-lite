// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.exec.ExecutionResult;
import com.legend.testbackend.TestBackend;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code #SQL{...}#} statements that write a table, executed end to end (docs/WRITES_DESIGN_2026_10_18.md §5, leg
 * 4), over a table with a primary key, which the parity corpus's tables have none of: ON CONFLICT (key) DO NOTHING and
 * DO UPDATE SET of the EXCLUDED values (L1's merge), an ON CONFLICT on no unique index as Postgres's error, UPDATE and
 * DELETE, RETURNING (the rows staged before the write) and UPDATE ... FROM. Each database the writes are measured on
 * runs {@link #writes} in a test named for it (DatabaseOwners: a database is named only by its owners).
 */
class SqlWritesExecutionTest {

    private static final String MODEL = """
            ###Relational
            Database w::DB (
              Table ITEM (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), QTY INTEGER)
              Table NOTE (ID INTEGER, TEXT VARCHAR(20))
            )
            ###Connection
            RelationalDatabaseConnection w::Conn {
              store: w::DB;
              %s
            }
            ###Runtime
            Runtime w::RT { mappings: [ ]; connections: [ w::DB: [ k: w::Conn ] ]; }
            """;

    private static final String ITEM = "tb('w::DB.ITEM')";

    /** Every check, on {@code backend}'s own workspace; {@code correlatedSets}: whether its database reads a SET's
     *  correlated scalar subquery (UPDATE ... FROM's), which reaches the statement's row from a derived table. */
    static void writes(TestBackend backend, boolean correlatedSets) throws Exception {
        try (Connection c = backend.openWorkspace()) {
            run(c, "CREATE TABLE ITEM (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), QTY INTEGER)");
            run(c, "CREATE TABLE NOTE (ID INTEGER, TEXT VARCHAR(20))");
            String name = backend.name();
            assertEquals(2L, count(backend, c, "INSERT INTO " + ITEM + " (ID, NAME, QTY) VALUES (1, 'a', 10),"
                    + " (2, 'b', 20)"), name);
            // DO NOTHING: the rows whose key the table holds are not written
            assertEquals(1L, count(backend, c, "INSERT INTO " + ITEM + " (ID, NAME, QTY) VALUES (2, 'B', 0),"
                    + " (3, 'c', 30) ON CONFLICT (ID) DO NOTHING"), name);
            assertEquals(List.of("1|a|10", "2|b|20", "3|c|30"), items(c), name);
            // DO UPDATE SET of the EXCLUDED values: merged by the key
            assertEquals(2L, count(backend, c, "INSERT INTO " + ITEM + " (ID, NAME, QTY) VALUES (1, 'A', 11),"
                    + " (4, 'd', 40) ON CONFLICT (ID) DO UPDATE SET NAME = EXCLUDED.NAME, QTY = EXCLUDED.QTY"), name);
            assertEquals(List.of("1|A|11", "2|b|20", "3|c|30", "4|d|40"), items(c), name);
            // a conflict on no unique index: Postgres's error
            Exception noIndex = assertThrows(Exception.class, () -> count(backend, c, "INSERT INTO " + ITEM
                    + " (ID, NAME) VALUES (5, 'e') ON CONFLICT (NAME) DO NOTHING"));
            assertTrue(String.valueOf(noIndex.getMessage()).contains("no unique or exclusion constraint"),
                    noIndex.getMessage());
            assertEquals(2L, count(backend, c, "UPDATE " + ITEM + " SET QTY = QTY + 1 WHERE ID > 2"), name);
            assertEquals(List.of("1|A|11", "2|b|20", "3|c|31", "4|d|41"), items(c), name);
            // RETURNING: the rows the statement wrote
            assertEquals(List.of(List.of("5", "100")), rows(backend, c, "INSERT INTO " + ITEM + " (ID, QTY) VALUES"
                    + " (5, 50) RETURNING ID, QTY * 2 AS DOUBLED"), name);
            assertEquals(List.of(List.of("4", "d")), rows(backend, c, "DELETE FROM " + ITEM + " WHERE ID = 4"
                    + " RETURNING ID, NAME"), name);
            assertEquals(List.of(List.of("3", "z", "31")), rows(backend, c, "UPDATE " + ITEM + " SET NAME = 'z'"
                    + " WHERE ID = 3 RETURNING *"), name);
            // DELETE ... USING: the rows a row of the other table meets the condition with
            run(c, "INSERT INTO NOTE VALUES (1, 'one'), (9, 'nine')");
            assertEquals(1L, count(backend, c, "DELETE FROM " + ITEM + " AS i USING tb('w::DB.NOTE') n"
                    + " WHERE n.ID = i.ID"), name);
            assertEquals(List.of("2|b|20", "3|z|31", "5|null|50"), items(c), name);
            if (correlatedSets) {
                // UPDATE ... FROM: each row a row of the other table meets the condition with, from it
                run(c, "INSERT INTO NOTE VALUES (2, 'two'), (5, 'five')");
                assertEquals(2L, count(backend, c, "UPDATE " + ITEM + " AS i SET NAME = n.TEXT FROM tb('w::DB.NOTE')"
                        + " n WHERE n.ID = i.ID"), name);
                assertEquals(List.of("2|two|20", "3|z|31", "5|five|50"), items(c), name);
            }
            // PR #67's second review (2026-10-24), as Postgres 16 answers: DO UPDATE's rows each of their key once
            // (DuckDB's MERGE had applied both); a key compared as the column holds it (2.4 is 2, which the table
            // holds); the target named as the proposed rows, ambiguous
            List<String> before = items(c);
            Exception twice = assertThrows(Exception.class, () -> count(backend, c, "INSERT INTO " + ITEM
                    + " (ID, NAME) VALUES (2, 'x'), (2, 'y') ON CONFLICT (ID) DO UPDATE SET NAME = EXCLUDED.NAME"),
                    name);
            assertEquals(before, items(c), name + ": " + twice.getMessage());
            assertEquals(0L, count(backend, c, "INSERT INTO " + ITEM + " (ID, NAME) VALUES (2.4, 'x')"
                    + " ON CONFLICT (ID) DO NOTHING"), name);
            assertEquals(before, items(c), name);
            assertEquals(1L, count(backend, c, "INSERT INTO " + ITEM + " (ID, NAME) VALUES (3.4, 'q')"
                    + " ON CONFLICT (ID) DO UPDATE SET NAME = EXCLUDED.NAME"), name);
            assertTrue(items(c).contains("3|q|31"), name + ": " + items(c));
            Exception excluded = assertThrows(Exception.class, () -> count(backend, c, "INSERT INTO " + ITEM
                    + " AS excluded (ID, NAME) VALUES (2, 'n') ON CONFLICT (ID) DO UPDATE SET NAME = excluded.NAME"),
                    name);
            assertTrue(String.valueOf(excluded.getMessage()).contains("ambiguous"), excluded.getMessage());
        }
    }

    private static ExecutionResult execute(TestBackend backend, Connection c, String sql) {
        return com.legend.Execution.execute(MODEL.formatted(backend.connectionBody()), "#SQL{" + sql + "}#", "w::RT",
                c);
    }

    private static Object count(TestBackend backend, Connection c, String sql) {
        return ((ExecutionResult.Scalar) java.util.Objects.requireNonNull(execute(backend, c, sql))).value();
    }

    private static List<List<String>> rows(TestBackend backend, Connection c, String sql) {
        List<List<String>> out = new ArrayList<>();
        for (var row : execute(backend, c, sql).rows()) {
            List<String> cells = new ArrayList<>();
            row.values().forEach(v -> cells.add(String.valueOf(v)));
            out.add(cells);
        }
        return out;
    }

    private static void run(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static List<String> items(Connection c) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT ID, NAME, QTY FROM ITEM ORDER BY ID")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + (rs.getObject(3) == null ? "null"
                        : rs.getInt(3)));
            }
        }
        return out;
    }
}
