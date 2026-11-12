// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/** H2's sessions, lite's H2 (2.5.252): a private in-memory instance each, with the engine's session settings
 *  (its dialect's session statements, {@code H2.sessionSetup}: until 2026-11-12 the connector's URL settings). A corpus workspace on H2 carries the engine's extension functions
 *  ({@link H2ExtensionFunctions}) as it did when it was also the replay referee's session; the referee is legend-engine's
 *  own H2 since 2026-11-05 ({@link EngineH2Referee}), and the H2 lane's goldens replay on its mirror as every lane's. */
final class H2Backend extends InProcessBackend {

    static final H2Backend INSTANCE = new H2Backend();

    /** The corpus's workspaces on H2, named apart ({@code c2s<N>}). */
    private final AtomicInteger workspaces = new AtomicInteger();

    private H2Backend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.H2;
    }

    @Override
    public String name() {
        return "h2";
    }

    /** A session of lite's H2 named {@code name}, with the engine's extension functions. */
    private static Connection withExtensions(String name) throws SQLException {
        // the session as a plan's target opens it (H2's settings its dialect's session statements: SHOULD-FIX 3)
        Connection referee = withSession(DriverManager.getConnection("jdbc:h2:mem:" + name, "sa", ""),
                new com.legend.sql.dialect.H2());
        // the engine's H2 test database carries its extension functions, and the corpus's raw text may call them
        // (legend_h2_extension_*)
        try (Statement st = referee.createStatement()) {
            for (String alias : H2ExtensionFunctions.aliases()) {
                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                st.execute(alias);
            }
        } catch (SQLException e) {
            referee.close();
            throw e;
        }
        return referee;
    }

    /** A corpus workspace on H2: a fresh session with the engine's extension functions (until 2026-11-05 a golden ran
     *  on it, the same-session referee; Phase 0.6: a golden calling {@code legend_h2_extension_lpad} was a FAULT). */
    @Override
    public Connection openWorkspace() throws SQLException {
        return withExtensions("c2s" + workspaces.getAndIncrement());
    }
}
