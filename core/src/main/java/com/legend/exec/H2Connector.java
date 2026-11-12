// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * H2's sessions: a file, a server, or an in-memory database that outlives its connections by NAME
 * ({@link Sessions.Opening.Named}). Every in-memory H2 database the platform opens, named or private, is opened with
 * the settings the plan's target declares ({@link #url}: the H2 dialect's {@code MODE=LEGACY} and
 * {@code DEFAULT_NULL_ORDERING=HIGH}, plan data since the C checkpoint's SHOULD-FIX 3, 2026-11-12; until then this
 * connector's {@code SETTINGS}, outside every plan, with the non-keywords that are now the dialect's session
 * statement); a file's or a server's URL is opened as the connection declares it, and the session checked against the
 * target's settings ({@code JdbcStatements.check}).
 *
 * <p>THE H2 session — the engine's own, VERBATIM (convergence batch C,
 * user-ratified "converge directly", landed 2026-08-29): H2Defaults
 * from legend-engine-xt-relationalStore-h2-execution-2.1.214 —
 * case-SENSITIVE identifiers, no DATABASE_TO_UPPER override, the
 * engine's NON_KEYWORDS list (incl OVER), MODE=LEGACY. ONE definition
 * (the H2 dialect's, written into every plan's target) so all targets
 * open IDENTICAL sessions.
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
    public Sessions.Opening opening(com.legend.executionplan.Connection def) {
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
    public Connection openPrivate(com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:private" + PRIVATE_IDS.getAndIncrement() + url(settings), "sa",
                "");
    }

    /** {@code settings} on an in-memory database's URL, each {@code ;NAME=VALUE}, as the plan's target declares them (the
     *  C checkpoint's SHOULD-FIX 3, 2026-11-12: H2's {@code MODE} and {@code DEFAULT_NULL_ORDERING}, the database's,
     *  whose {@code SET} would commit a caller's transaction); none for null. A name or value that is not a plain word
     *  is refused by name: it would write another setting. */
    static String url(com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) {
        if (settings == null) {
            return "";
        }
        StringBuilder url = new StringBuilder();
        for (com.legend.executionplan.ExecutionPlan.Setting s : settings.required()) {
            if (!s.name().matches("[A-Za-z_][A-Za-z0-9_]*") || !s.value().matches("[A-Za-z0-9_]+")) {
                throw new IllegalArgumentException("an H2 database setting is a name and a value, each a plain word: "
                        + s);
            }
            url.append(';').append(s.name()).append('=').append(s.value());
        }
        return url.toString();
    }

    @Override
    public String storelessDeclaration() {
        return "specification: LocalH2 { };\n  auth: DefaultH2";
    }

    @Override
    public Connection openNamed(String name, com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) throws SQLException {
        return named(name, ";DB_CLOSE_DELAY=-1", settings);
    }

    @Override
    public Connection openKept(String name, com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) throws SQLException {
        return named(name, "", settings);
    }

    @Override
    public Connection joinKept(String name, com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) throws SQLException {
        return named(name, ";IFEXISTS=TRUE", settings);
    }

    private static Connection named(String name, String closeDelay, com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings)
            throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:" + name + closeDelay + url(settings));
    }

    /** {@inheritDoc} No: H2 rolls back the failed statement alone; its transaction goes on. */
    @Override
    public boolean abortsTransactionOnError() {
        return false;
    }

    /** {@inheritDoc} Yes: its driver takes autocommit off, a commit and a rollback. */
    @Override
    public boolean takesTransactions() {
        return true;
    }
}
