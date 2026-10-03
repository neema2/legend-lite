// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testing.EmbeddedPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The database a PCT lane runs its suite on, chosen by the target ({@code LEGENDLITE_PCT_BACKEND}, an
 * environment variable: it must survive the fork into the test JVM), one connection per executed
 * expression:
 *
 * <ul>
 *   <li>unset: a fresh in-memory DuckDB;</li>
 *   <li>{@code h2}: a fresh in-memory H2, with the portability sweep's session settings;</li>
 *   <li>{@code postgres}: this JVM's embedded Postgres 16 (leg P2), a new session on its database.</li>
 * </ul>
 *
 * Session settings are the dialect's, applied at the platform's connection seam
 * ({@code SqlDialect.sessionSetup}, from {@code Compiler.dialectOf}); this only opens the connection.
 */
final class PctBackend {

    private PctBackend() {
    }

    /** The lanes' databases: each one's declared type (the storeless runtime's connection) and its session. */
    private enum Lane {
        DUCKDB(DatabaseType.DuckDB) {
            @Override
            Connection open() throws SQLException {
                return DriverManager.getConnection("jdbc:duckdb:");
            }
        },
        H2(DatabaseType.H2) {
            @Override
            Connection open() throws SQLException {
                return DriverManager.getConnection("jdbc:h2:mem:" + com.legend.exec.H2Settings.SETTINGS, "sa", "");
            }
        },
        POSTGRES(DatabaseType.Postgres) {
            @Override
            Connection open() throws SQLException {
                return DriverManager.getConnection(EmbeddedPostgres.shared().jdbcUrl("postgres"));
            }
        };

        final DatabaseType type;

        Lane(DatabaseType type) {
            this.type = type;
        }

        abstract Connection open() throws SQLException;
    }

    private static Lane lane() {
        String backend = System.getenv("LEGENDLITE_PCT_BACKEND");
        if (backend == null) {
            return Lane.DUCKDB;
        }
        return switch (backend) {
            case "h2" -> Lane.H2;
            case "postgres" -> Lane.POSTGRES;
            default -> throw new IllegalStateException("LEGENDLITE_PCT_BACKEND=" + backend + ": a PCT lane runs on"
                    + " DuckDB (unset), h2 or postgres");
        };
    }

    /** The database this lane runs on: what its storeless runtime declares. */
    static DatabaseType databaseType() {
        return lane().type;
    }

    /** A session on this lane's database. */
    static Connection connect() throws SQLException {
        return lane().open();
    }
}
