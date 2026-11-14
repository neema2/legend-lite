// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

/** Snowflake's sessions: a real account's warehouse ({@link CloudBackend}): its {@code account}, {@code warehouse},
 *  {@code region} and {@code database} declared; the system connection's {@code schema}, where its temporary tables
 *  live. */
final class SnowflakeBackend extends CloudBackend {

    static final SnowflakeBackend INSTANCE = new SnowflakeBackend();

    private SnowflakeBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.Snowflake;
    }

    @Override
    public String name() {
        return "snowflake";
    }

    @Override
    String specificationAndAuth(Properties p) {
        return "specification: Snowflake { name: " + quoted(p, "database") + "; account: "
                + quoted(p, "account") + "; warehouse: " + quoted(p, "warehouse") + "; region: "
                + quoted(p, "region") + "; };\n  auth: SnowflakePublic { publicUserName: 'snowflake.user';"
                + " privateKeyVaultReference: 'snowflake.privateKey'; passPhraseVaultReference:"
                + " 'snowflake.passPhrase'; }";
    }

    /** {@inheritDoc} A NEW session ({@link #openSystem}'s) in a schema made for it in the account's database, named
     *  {@code LITE_WS_<a random UUID>}: a test's tables are its own, beside other workspaces; closing the workspace drops
     *  the schema with everything in it, then closes the session (docs/TS_DECISIONS_2026_11_07.md item 10, 2026-11-07:
     *  the #TS island on Snowflake). */
    @Override
    public Connection openWorkspace() throws SQLException {
        Connection session = openSystem();
        String schema = "LITE_WS_" + UUID.randomUUID().toString().replace('-', '_').toUpperCase(Locale.ROOT);
        try (Statement st = session.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("CREATE SCHEMA " + schema);
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("USE SCHEMA " + schema);
        } catch (SQLException e) {
            try (Statement st = session.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            } catch (SQLException dropped) {
                e.addSuppressed(dropped);
            } finally {
                session.close();
            }
            throw e;
        }
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close") && method.getParameterCount() == 0) {
                        if (!session.isClosed()) {
                            try (Statement st = session.createStatement()) {
                                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                                st.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
                            } finally {
                                session.close();
                            }
                        }
                        return null;
                    }
                    try {
                        return method.invoke(session, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
