// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.testbackend.TestBackend;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Staged rows (lite's {@code stage}, docs/WRITES_DESIGN_2026_10_18.md §8, leg 6): {@code let s = rows->stage()}
 * computes the rows once, at the let's turn among a program's statements, into a temporary table of the session, and
 * the statements after the let read that table: a write between them does not change the rows the let holds. The
 * table is the session's own and is dropped when the program has run.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class StagedRowsTest {

    private static final String MODEL = """
            ###Relational
            Database w::DB (
              Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)
              Table SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)
            )
            ###Connection
            RelationalDatabaseConnection w::Conn {
              store: w::DB;
              %s
            }
            ###Runtime
            Runtime w::RT { mappings: [ ]; connections: [ w::DB: [ c: w::Conn ] ]; }
            """;

    private static final TestBackend BACKEND = LaneFloor.backend();

    @Test
    void aStagedLetHoldsItsRowsAcrossTheWritesAfterIt() throws Exception {
        try (Connection c = BACKEND.openWorkspace()) {
            run(c, "CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)");
            run(c, "CREATE TABLE SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)");
            run(c, "INSERT INTO SRC VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
            // the staged rows are the ones before the delete: the let holds them
            Lean.Result r = execute(c, "{|let s = #>{w::DB.SRC}#->filter(r|$r.AGE > 10)"
                    + "->meta::legend::lite::stage(); #>{w::DB.SRC}#->delete(r|true); $s->sort(~ID->ascending());}");
            assertEquals(List.of("2|b|20", "3|c|30"), cells(r));
            assertEquals(0, count(c, "SRC"));
            // the session's table is dropped once the program has run
            assertThrows(Exception.class, () -> run(c, "SELECT * FROM legend_stage_s"));
            // a staged let read by a write: the rows written are the staged ones
            run(c, "INSERT INTO SRC VALUES (4, 'd', 40)");
            execute(c, "{|let s = #>{w::DB.SRC}#->meta::legend::lite::stage(); #>{w::DB.SRC}#->delete(r|true);"
                    + " $s->write(#>{w::DB.T}#);}");
            assertEquals(1, count(c, "T"));
            // a stage outside a let is refused by name
            Exception e = assertThrows(Exception.class, () -> execute(c,
                    "|#>{w::DB.SRC}#->meta::legend::lite::stage()"));
            assertTrue(String.valueOf(e.getMessage()).contains("a let's value"), e.getMessage());
        }
    }

    @Test
    void aDeleteOfTheRowsAnotherRelationNames() throws Exception {
        try (Connection c = BACKEND.openWorkspace()) {
            run(c, "CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)");
            run(c, "CREATE TABLE SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)");
            run(c, "INSERT INTO T VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
            run(c, "INSERT INTO SRC VALUES (2, 'b', 20)");
            execute(c, "|#>{w::DB.T}#->delete(r|#>{w::DB.SRC}#->filter(s|$s.ID == $r.ID)->size() > 0)");
            assertEquals(2, count(c, "T"));
        }
    }

    private static Lean.Result execute(Connection c, String query) {
        return Program.last(MODEL.formatted(BACKEND.connectionBody()), query, "w::RT", c);
    }

    private static List<String> cells(Lean.Result r) {
        List<String> out = new ArrayList<>();
        for (var row : r.rows()) {
            List<String> cs = new ArrayList<>();
            row.forEach(v -> cs.add(String.valueOf(v)));
            out.add(String.join("|", cs));
        }
        return out;
    }

    private static int count(Connection c, String table) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static void run(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }
}
