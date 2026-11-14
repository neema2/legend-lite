// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.AssertFailed;
import com.legend.testing.LaneFloor;
import com.legend.testing.Program;
import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A RELATION'S CELLS SORTED ({@code $r.values.rows.values->sort()}, the corpus's
 * {@code meta::relational::tests::mapping::dates::strictdate::testProject}): the cells of an Integer column and a
 * StrictDate column are one mixed list, sorted by Pure's compare across kinds (the numbers, then the dates). The list
 * is over the relation's rows, and its sort renumbers those rows (2026-11-06): no array read at a position, which H2's
 * list-less dialect refuses ("LIST_AT reached a dialect without a list encoding"), and no window nested in the root's
 * order, which DuckDB refuses under the database judge ("window functions are not allowed in window definitions").
 * Each case runs on the lane's database under the lane's judge.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class RelationCellsSortTest {

    private static final String MODEL = """
            ###Pure
            Class m::Trade
            {
                id: Integer[1];
                date: StrictDate[1];
            }
            ###Relational
            Database s::DB ( Table T (ID INTEGER PRIMARY KEY, D DATE) )
            ###Mapping
            Mapping m::M (
              m::Trade: Relational { ~mainTable [s::DB] T id: T.ID, date: T.D }
            )
            ###Connection
            RelationalDatabaseConnection s::C { store: s::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c0: s::C ] ]; }
            """;

    /** Three trades, their ids and dates in different orders. */
    private static Connection seeded() throws Exception {
        Connection c = LaneFloor.backend().open();
        try (Statement st = c.createStatement()) {
            st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, D DATE)");
            st.execute("INSERT INTO T VALUES (1, '2016-02-04')");
            st.execute("INSERT INTO T VALUES (2, '2014-12-01')");
            st.execute("INSERT INTO T VALUES (3, '2014-12-02')");
        }
        return c;
    }

    private static String body(String expected) {
        return "{|let r = execute(|m::Trade.all()->project([t|$t.id, t|$t.date], ['id', 'date']), m::M,"
                + " m::RT.runtimeValue, []); assertEquals(" + expected + ", $r.values.rows.values->sort());}";
    }

    @Test
    @DisplayName("a relation's Integer and StrictDate cells sort as one list: the numbers, then the dates")
    void cellsSortNumbersThenDates() throws Exception {
        try (Connection c = seeded()) {
            assertTrue(Program.run(MODEL, body("[1, 2, 3, %2014-12-01, %2014-12-02, %2016-02-04]"), "m::RT", c)
                    .passed());
        }
    }

    @Test
    @DisplayName("the sorted cells are in order: the same values in another order fail")
    void cellsOutOfOrderFail() throws Exception {
        try (Connection c = seeded()) {
            assertThrows(AssertFailed.class, () -> Program.run(MODEL,
                    body("[1, 2, 3, %2016-02-04, %2014-12-01, %2014-12-02]"), "m::RT", c));
        }
    }
}
