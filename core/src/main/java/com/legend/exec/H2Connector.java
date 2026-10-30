// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * H2's sessions: a file, a server, or an in-memory database that outlives its connections by NAME
 * ({@link Sessions.Opening.Named}). Every in-memory H2 session the platform opens, named or private, takes
 * {@link #SETTINGS} (moved here from {@code H2Settings}, the dialect program's Phase 5, 2026-10-16); a file's or a
 * server's URL is opened as the connection declares it.
 *
 * <p>THE H2 session — the engine's own, VERBATIM (convergence batch C,
 * user-ratified "converge directly", landed 2026-08-29): H2Defaults
 * from legend-engine-xt-relationalStore-h2-execution-2.1.214 —
 * case-SENSITIVE identifiers, no DATABASE_TO_UPPER override, the
 * engine's NON_KEYWORDS list (incl OVER), MODE=LEGACY. ONE definition
 * shared by every H2 session opener so all targets open IDENTICAL
 * sessions.
 *
 * <p>HISTORY: this constant used to add CASE_INSENSITIVE_IDENTIFIERS
 * + DATABASE_TO_UPPER=false — session-level compensation for OUR OWN
 * spelling skew (create full-quoted vs insert bare; renderer quoting
 * what DDL spelled bare; bare aliases uppercasing in labels). The
 * cure was conform-by-emission end to end: per-target DDL/insert
 * spelling, declared-quote preservation, and the ORIGIN-driven
 * renderer (a column reference knows whether its name is PHYSICAL —
 * DDL-owned, bare-unless-special — or DERIVED — query-invented,
 * quoted like the engine spells every alias). Receipts: all four
 * consumers probed green on THIS session (referee sweep
 * census-byte-identical; h2 lane 1369; PCT DuckDB + PCT h2modern).
 */
public final class H2Connector implements DatabaseConnector, DatabaseConnector.NamedMemory {

    static final H2Connector INSTANCE = new H2Connector();

    /** JDBC-URL suffix, {@code ;KEY=VALUE} form — the engine's own H2
     *  connection settings (H2Manager / H2Defaults, 4.145.0), so a golden
     *  replayed here orders as it did in the engine's run:
     *  {@code DEFAULT_NULL_ORDERING=HIGH} is the engine's canonical null
     *  placement (null is largest — ASC nulls last, DESC nulls first; batch
     *  8, the eleven testGroupBy.pure desc sorts) which its printer leaves
     *  BARE on H2 2.x because the session already orders that way. */
    // NOT a compile-time constant (String.join, not a literal): a literal
    // would be INLINED into every class that reads it — the spec module's
    // test classes kept the pre-4.145.0 value across a core install until
    // a clean (batch 8: two chains chased a null-ordering divergence that
    // was a stale inlined string)
    public static final String SETTINGS = String.join("",
            ";NON_KEYWORDS=ANY,ASYMMETRIC,AUTHORIZATION,CAST,",
            "CURRENT_PATH,CURRENT_ROLE,DAY,DEFAULT,ELSE,END,HOUR,KEY,",
            "MINUTE,MONTH,SECOND,SESSION_USER,SET,SOME,SYMMETRIC,",
            "SYSTEM_USER,TO,UESCAPE,USER,VALUE,WHEN,YEAR,OVER",
            ";MODE=LEGACY",
            ";DEFAULT_NULL_ORDERING=HIGH");

    private static final AtomicInteger PRIVATE_IDS = new AtomicInteger();

    private H2Connector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.H2;
    }

    @Override
    public String jdbcProduct() {
        return "H2";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        ConnectionSpecification spec = def.specification();
        if (spec instanceof ConnectionSpecification.LocalFile(String path)) {
            return new Sessions.Opening.Url("jdbc:h2:file:" + path);
        }
        if (spec instanceof ConnectionSpecification.StaticDatasource(String host, int port, String database)) {
            return new Sessions.Opening.Url("jdbc:h2:tcp://" + host + ":" + port + "/" + database);
        }
        // A19: a DISTINCT in-memory database per databaseName -- the engine's directory-backed isolation without disk
        // side effects; a USER-NAMED database is user-chosen identity and shares BY DESIGN
        if (spec instanceof ConnectionSpecification.EmbeddedH2 embedded) {
            return new Sessions.Opening.Named(this, embedded.databaseName());
        }
        // legend-engine's test specification and an in-memory one: the opener names it
        if (spec instanceof ConnectionSpecification.LocalH2 || spec instanceof ConnectionSpecification.InMemory) {
            return new Sessions.Opening.Named(this, null);
        }
        return DatabaseConnector.refused(def);
    }

    @Override
    public boolean inProcess() {
        return true;
    }

    @Override
    public Connection openPrivate() throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:private" + PRIVATE_IDS.getAndIncrement() + SETTINGS, "sa", "");
    }

    @Override
    public String storelessDeclaration() {
        return "specification: LocalH2 { };\n  auth: DefaultH2";
    }

    @Override
    public Connection openNamed(String name) throws SQLException {
        return named(name, ";DB_CLOSE_DELAY=-1");
    }

    @Override
    public Connection openKept(String name) throws SQLException {
        return named(name, "");
    }

    @Override
    public Connection joinKept(String name) throws SQLException {
        return named(name, ";IFEXISTS=TRUE");
    }

    private static Connection named(String name, String closeDelay) throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:" + name + closeDelay + SETTINGS);
    }

    /** {@inheritDoc} None: {@code H2}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }
}
