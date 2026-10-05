package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.EmbeddedPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The server's Postgres arm ({@link ConnectionResolver}: a {@code type: Postgres} connection with a static
 * datasource) opens a real Postgres through the product's own driver set ({@code //core:drivers}). Until
 * 2026-10-03 that set held no Postgres driver, so the arm failed with "No suitable driver" in the server
 * and in its deploy jar (Bazel workplan P0-08). Its own target ({@code //core:postgres_arm_test}): it
 * needs the embedded Postgres in its runfiles, which {@code //core:core_tests} does not carry.
 */
class PostgresArmTest {

    @Test
    @DisplayName("a Postgres connection resolves through the product path and runs a query")
    void thePostgresArmOpensARealPostgres() throws Exception {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        // The arm sends no user (its connections authenticate as Test or NoAuth), so the driver logs in
        // as the operating-system account: give that account a login role on the test's own server.
        String account = System.getProperty("user.name");
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"));
                PreparedStatement exists = admin.prepareStatement("SELECT 1 FROM pg_roles WHERE rolname = ?")) {
            exists.setString(1, account);
            try (ResultSet r = exists.executeQuery()) {
                if (!r.next()) {
                    try (Statement create = admin.createStatement()) {
                        create.execute("CREATE ROLE \"" + account.replace("\"", "\"\"") + "\" LOGIN");
                    }
                }
            }
        }

        String model = String.format(java.util.Locale.ROOT, """
                ###Relational
                Database store::PgDB ( Table T ( ID INTEGER PRIMARY KEY ) )

                ###Connection
                RelationalDatabaseConnection store::PgConn {
                    type: Postgres;
                    specification: Static { host: '127.0.0.1'; port: %d; name: 'postgres'; };
                    auth: Test;
                }

                ###Runtime
                Runtime test::PgRuntime {
                    mappings: [ ];
                    connections: [ store::PgDB: [ environment: store::PgConn ] ];
                }
                """, pg.port());

        try (ConnectionResolver.Lease lease = ConnectionResolver.lease(com.legend.Compiler.compileModel(model), "test::PgRuntime");
                Statement s = lease.connection().createStatement();
                ResultSet r = s.executeQuery("SELECT 41 + 1")) {
            assertTrue(lease.connection().getMetaData().getURL().startsWith("jdbc:postgresql://127.0.0.1:"),
                    lease.connection().getMetaData().getURL());
            assertTrue(r.next());
            assertEquals(42, r.getInt(1));
        }
    }
}
