// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDdl;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A float's and a bit's cell of model data, as upstream writes them (the A checkpoint's audits, T9 and C1,
 * 2026-11-10; read from the pinned sources: {@code convertValuesToCsv} writes a number's cell as its raw token, H2's
 * {@code bitValueFromString} reads y, 1 and true as true and n, 0 and false as false, trimmed and in any case, any other
 * cell raw): a float's digits as written, which the database assigns (until then parsed to a double, so 1e400 became
 * Infinity and an unnamed failure); a float's cell that is no number refused by name (upstream's raw token is no SQL
 * value); a bit's y and n, and any other number its digits.
 */
class RowLoadCellsTest {

    private static final List<SqlDdl.ColumnType> TYPES = List.of(
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER),
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.DOUBLE),
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.BIT));

    private static RowLoad load(List<List<String>> rows) {
        return new RowLoad(null, "CELLS", NameQuoting.BARE, List.of("I", "F", "B"), Set.of(), 3, rows, TYPES);
    }

    @Test
    void aFloatsDigitsAndABitsSpellingsLoadAsUpstreamWritesThem_onH2() throws Exception {
        RowLoad rows = load(List.of(List.of("1", "1e3", "y"), List.of("2", "-2.5", " N "), List.of("3", "0.1", "2"),
                List.of("4", "12345678901234567890", "TRUE")));
        try (Connection c = TestBackends.of(DatabaseType.H2).open(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE CELLS (I INTEGER, F DOUBLE PRECISION, B BOOLEAN)");
            try {
                for (String sql : rows.statements(Databases.dialect(DatabaseType.H2))) {
                    st.execute(sql);
                }
                List<String> got = new ArrayList<>();
                try (ResultSet rs = st.executeQuery("SELECT I, F, B FROM CELLS ORDER BY I")) {
                    while (rs.next()) {
                        got.add(rs.getInt(1) + "|" + rs.getDouble(2) + "|" + rs.getBoolean(3));
                    }
                }
                assertEquals(List.of("1|1000.0|true", "2|-2.5|false", "3|0.1|true", "4|1.2345678901234567E19|true"),
                        got);
            } finally {
                st.execute("DROP TABLE CELLS");
            }
        }
    }

    @Test
    void aFloatsOrABitsCellThatIsNoNumberIsRefusedByName_aHugeNumberIsTheDatabasesToAssign() {
        var nan = assertThrows(com.legend.error.DataError.class, () -> load(List.of(List.of("1", "NaN", "true")))
                .statements(Databases.dialect(DatabaseType.H2)));
        assertTrue(nan.getMessage().contains("column F is DOUBLE, and its data holds 'NaN'"), nan.getMessage());
        var maybe = assertThrows(com.legend.error.DataError.class, () -> load(List.of(List.of("1", "1", "maybe")))
                .statements(Databases.dialect(DatabaseType.H2)));
        assertTrue(maybe.getMessage().contains("column B is BIT, and its data holds 'maybe'"), maybe.getMessage());
        // planned, its digits kept: what a DOUBLE does with it is the database's (until then an unnamed failure)
        String huge = String.join("\n", load(List.of(List.of("1", "1e400", "true")))
                .statements(Databases.dialect(DatabaseType.H2)));
        assertTrue(huge.contains("'1e400'"), huge);
    }

    /** A cell in exponent form is a float of its digits as written on every database, read by the database (the A delta
     *  audit's 3, 2026-11-11: as an exact decimal, BigQuery refused 1e300's 301 digits, ClickHouse overflowed 1.5E10 as a
     *  Decimal(2,0) and Trino wrote 1e-50 as 51 digits); a plain cell stays its exact digits. */
    @Test
    void anExponentCellIsAFloatOfItsDigitsOnEveryDatabase() {
        for (DatabaseType db : List.of(DatabaseType.BigQuery, DatabaseType.ClickHouse, DatabaseType.Trino,
                DatabaseType.DuckDB, DatabaseType.Postgres, DatabaseType.H2)) {
            for (String cell : List.of("1e300", "1.5E10", "1e-50")) {
                String sql = String.join("\n", load(List.of(List.of("1", cell, "true")))
                        .statements(Databases.dialect(db)));
                assertTrue(sql.contains("'" + cell + "'") && !sql.matches("(?s).*\\d{40,}.*"), db + ": " + sql);
            }
            String plain = String.join("\n", load(List.of(List.of("1", "2.5", "true"))).statements(Databases.dialect(db)));
            assertTrue(plain.contains("2.5") && !plain.toUpperCase(java.util.Locale.ROOT).contains("DOUBLE"),
                    db + ": " + plain);
        }
    }
}
