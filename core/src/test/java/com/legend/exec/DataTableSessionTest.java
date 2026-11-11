// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.sql.SqlDdl;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code data:} source's SESSION TABLE as each in-process database spells it ({@code SqlDialect.dataTable}; B2a and
 * the B2a audit's S3, 2026-11-11), run on the database itself: prepared, made, filled with two records -- a quote and a
 * line break in their text -- read in their places, released; a USER'S table of the same name in the session's own
 * schema is neither read nor dropped; the session holds no table of the name after the release; and a release where
 * the table was never made raises nothing (H2's schema: the audit's note).
 */
class DataTableSessionTest {

    private static final String BASE = "legend_data_0123456789abcdef_o";
    private static final List<SqlDdl.Column> COLUMNS = List.of(
            new SqlDdl.Column("data", false, new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.JSON), false, false),
            new SqlDdl.Column("u_frame_ord__", false, new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER),
                    false, false));

    @Test
    @DisplayName("each database's session table holds the plan's records and never a user's table of its name")
    void aSessionTableIsTheSessionsOwn() throws SQLException {
        int spelled = 0;
        for (com.legend.testbackend.TestBackend backend : TestBackends.inProcess()) {
            SqlDialect dialect = backend.dialect();
            SqlDialect.DataTable table;
            try {
                table = dialect.dataTable(BASE, COLUMNS);
            } catch (com.legend.sql.DialectCapability refused) {
                continue;   // no session table spelled: a data: source is refused there by name
            }
            spelled++;
            String name = backend.type().name();
            try (Connection c = backend.open(); Statement s = c.createStatement()) {
                // a release where nothing was made raises nothing (H2's schema, never made: the audit's note)
                for (String statement : table.release()) {
                    assertDoesNotThrow(() -> s.execute(statement), name + ": " + statement);
                }
                // the user's own table of the name, unqualified -- the session's own schema -- holding the user's row
                s.execute("CREATE TABLE " + table.name() + " (data VARCHAR(20))");
                s.execute("INSERT INTO " + table.name() + " VALUES ('the user''s row')");
                List<String> sent = new ArrayList<>(table.prepare());
                sent.addAll(table.make());
                sent.addAll(new com.legend.setup.RowLoad(table.schema(), table.name(), com.legend.sql.NameQuoting.BARE,
                        List.of("data", "u_frame_ord__"), Set.of(), 2, List.of(
                                List.of("{\"t\":\"it's\"}", "0"), List.of("{\"t\":\"a\\nb\"}", "1")),
                        List.of(COLUMNS.get(0).type(), COLUMNS.get(1).type())).statements(dialect));
                for (String statement : sent) {
                    s.execute(statement);
                }
                List<String> read = new ArrayList<>();
                try (ResultSet r = s.executeQuery("SELECT u_frame_ord__ FROM " + table.read()
                        + " ORDER BY u_frame_ord__")) {
                    while (r.next()) {
                        read.add(r.getString(1));
                    }
                }
                assertEquals(List.of("0", "1"), read, name + ": the plan's records, in their places");
                for (String statement : table.release()) {
                    s.execute(statement);
                }
                // the user's row as it was
                try (ResultSet r = s.executeQuery("SELECT data FROM " + table.name())) {
                    r.next();
                    assertEquals("the user's row", r.getString(1), name + ": the user's table untouched");
                }
                // and the session's table gone: a qualified read finds nothing; an unqualified one finds the user's
                if (table.schema() != null) {
                    assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1 FROM " + table.read()),
                            name + ": the session's table dropped");
                } else {
                    try (ResultSet r = s.executeQuery("SELECT data FROM " + table.read())) {
                        r.next();
                        assertEquals("the user's row", r.getString(1), name + ": the session's table dropped");
                    }
                }
            }
        }
        assertTrue(spelled >= 2, "the in-process databases that spell a session table: " + spelled);
    }
}
