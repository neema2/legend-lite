// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The raise a session holds, read back by the seam (D65, 2026-10-29): MariaDB's and MySQL's connectors DECLARE the
 * two statements ({@link DatabaseConnector#raiseRecovery}: the read of {@code @legend_raise}, its reset) and
 * {@link JdbcStatements#held} runs them after a failure. H2 keeps session variables under the same spelling
 * ({@code SET @name = value}, {@code SELECT @name}), so the mechanism is measured here on the texts the connectors
 * declare, verbatim; the MariaDB and MySQL lanes themselves are manual ({@code //gates:heavy}).
 */
class RaiseRecoveryTest {

    private static final DatabaseConnector.RaiseRecovery MARIADB =
            new DatabaseConnector.RaiseRecovery("SELECT @legend_raise", "SET @legend_raise = NULL");

    @Test
    void mariaDbAndMySqlDeclareTheReadAndTheReset_everyOtherBuiltConnectorDeclaresNone() {
        assertEquals(MARIADB, Connectors.of(DatabaseType.MariaDB).raiseRecovery());
        assertEquals(MARIADB, Connectors.of(DatabaseType.MySQL).raiseRecovery());
        for (DatabaseType type : new DatabaseType[] {DatabaseType.H2, DatabaseType.DuckDB, DatabaseType.SQLite,
                DatabaseType.Postgres, DatabaseType.Trino, DatabaseType.ClickHouse, DatabaseType.Snowflake,
                DatabaseType.Databricks, DatabaseType.BigQuery, DatabaseType.MongoDB}) {
            assertNull(Connectors.of(type).raiseRecovery(), type.toString());
        }
    }

    @Test
    void theSeamReadsTheHeldTextOnceAndClearsIt() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:raise_" + System.nanoTime());
                Statement st = c.createStatement()) {
            assertNull(JdbcStatements.held(c, MARIADB), "nothing held yet");
            st.execute("SET @legend_raise = 'Cannot cast a collection of size 0 to multiplicity [1]'");
            assertEquals("Cannot cast a collection of size 0 to multiplicity [1]", JdbcStatements.held(c, MARIADB));
            assertNull(JdbcStatements.held(c, MARIADB), "the reset: a later failure reads no stale text");
            assertNull(JdbcStatements.held(c, null), "a database that declares none");
        }
    }

    @Test
    void aReadTheSessionRefusesIsNoText_theStatementsOwnErrorStands() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:h2:mem:raise_" + System.nanoTime());
        // held(Connection) finds the connector by the session's product: H2 declares none
        assertNull(JdbcStatements.held(c));
        c.close();
        // a session that cannot be asked (closed: its metadata read and any statement refused) holds no text, and
        // the refusal is swallowed -- a failure of the read is never the statement's error
        assertNull(JdbcStatements.held(c));
        SQLException refused = assertThrows(SQLException.class, () -> JdbcStatements.held(c, MARIADB),
                "the statements themselves, on a closed session, refuse");
        assertEquals("90007", refused.getSQLState(), "H2: the object is already closed");
    }

    @Test
    void theSessionIsAskedOnlyWhereTheErrorsOwnTextCarriesNoRaise() {
        int[] asked = {0};
        java.util.function.Supplier<String> held = () -> {
            asked[0]++;
            return RaisedErrors.SENTINEL + "the raise's text" + RaisedErrors.SENTINEL;
        };
        SQLException carried = new SQLException(RaisedErrors.SENTINEL + "carried" + RaisedErrors.SENTINEL, "45000");
        assertEquals("carried", RaisedErrors.unwrapped(carried, held).getMessage());
        assertEquals(0, asked[0], "the error carried its raise: the session is not asked");
        SQLException own = new SQLException("Subquery returns more than 1 row", "21000");
        SQLException told = RaisedErrors.unwrapped(own, held);
        assertEquals(1, asked[0], "no raise in the text: the session is asked once");
        assertEquals("the raise's text", told.getMessage());
        assertEquals("21000", told.getSQLState());
        assertSame(own, RaisedErrors.unwrapped(own, () -> null), "nothing held: the error as it is");
    }
}
