// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.NotImplementedException;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Every database's sessions, as its connector answers them (the dialect program's Phase 5, 2026-10-16, the audit of the
 * branch): each declared type with each connection specification and each authentication, its driver's product name,
 * its private instance and its storeless runtime, against what {@code Sessions} answered before the connectors (its
 * switches, 59b547a). A connector refuses every specification it does not open, so a NEW specification or
 * authentication fails {@link #everySpecificationAndAuthenticationIsInTheTable} until its row says what each database
 * does with it: the decision the old exhaustive switches forced.
 */
class ConnectorsTest {

    private static final String Q = "q::C";

    /** One of each specification. */
    private static final Map<Class<?>, ConnectionSpecification> SPECIFICATIONS = specifications();

    private static Map<Class<?>, ConnectionSpecification> specifications() {
        Map<Class<?>, ConnectionSpecification> m = new LinkedHashMap<>();
        for (ConnectionSpecification s : List.of(new ConnectionSpecification.InMemory(),
                new ConnectionSpecification.LocalFile("/tmp/x.db"), new ConnectionSpecification.LocalH2(null),
                new ConnectionSpecification.EmbeddedH2("shared", "/tmp/dir", false),
                new ConnectionSpecification.StaticDatasource("db.example", 5432, "app"),
                new ConnectionSpecification.Trino("trino.example", 8080, "memory", "default", null, false),
                new ConnectionSpecification.Snowflake("d", "a", "w", "r", null, null, null, null, null),
                new ConnectionSpecification.Spanner("p", "i", "d"),
                new ConnectionSpecification.Databricks("h", "443", "https", "/sql"),
                new ConnectionSpecification.BigQuery("p", "d"))) {
            m.put(s.getClass(), s);
        }
        return m;
    }

    /** One of each authentication, and whether a session needs no credential for it. */
    private static final Map<AuthenticationSpec, Boolean> AUTHENTICATIONS = authentications();

    private static Map<AuthenticationSpec, Boolean> authentications() {
        Map<AuthenticationSpec, Boolean> m = new LinkedHashMap<>();
        m.put(new AuthenticationSpec.NoAuth(), true);
        m.put(new AuthenticationSpec.DefaultH2(), true);
        m.put(new AuthenticationSpec.TestAuth(), true);
        m.put(new AuthenticationSpec.UsernamePassword("u", "p"), false);
        m.put(new AuthenticationSpec.DelegatedKerberos(null, null, null), false);
        m.put(new AuthenticationSpec.VaultUserNamePassword(null, "u", "p"), false);
        m.put(new AuthenticationSpec.SnowflakePublic("u", "k", "p"), false);
        m.put(new AuthenticationSpec.GCPApplicationDefaultCredentials(), false);
        m.put(new AuthenticationSpec.ApiToken("t"), false);
        m.put(new AuthenticationSpec.MiddleTierUserNamePassword("v"), false);
        m.put(new AuthenticationSpec.OAuth("k", "s"), false);
        m.put(new AuthenticationSpec.GcpWorkloadIdentityFederation("e", List.of()), false);
        return m;
    }

    private static Set<String> names(Class<?>[] classes) {
        return Arrays.stream(classes).map(Class::getName).collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    void everySpecificationAndAuthenticationIsInTheTable() {
        assertEquals(names(ConnectionSpecification.class.getPermittedSubclasses()),
                names(SPECIFICATIONS.keySet().toArray(Class<?>[]::new)),
                "a specification no row decides: say what each database's connector does with it");
        assertEquals(names(AuthenticationSpec.class.getPermittedSubclasses()),
                names(AUTHENTICATIONS.keySet().stream().map(Object::getClass).toArray(Class<?>[]::new)),
                "an authentication no row decides");
    }

    /** How {@code def} opens, or its refusal's message. */
    private static String opening(ConnectionDefinition def) {
        try {
            return switch (Sessions.openingFor(def)) {
                case Sessions.Opening.Url u -> "url " + u.url();
                case Sessions.Opening.Named n -> {
                    assertSame(H2Connector.INSTANCE, n.memory());
                    yield "named " + n.name();
                }
                case Sessions.Opening.Held h -> {
                    assertSame(Connectors.of(def.databaseType()), h.connector());
                    yield "held";
                }
            };
        } catch (NotImplementedException refused) {
            return "refused: " + refused.getMessage();
        }
    }

    /** What {@code Sessions.openingFor} answered for a type and a specification before the connectors. */
    private static String expected(DatabaseType type, ConnectionSpecification spec) {
        String refused = "refused: a " + type + " connection with a " + spec.getClass().getSimpleName()
                + " specification is not implemented (connection '" + Q + "')";
        Function<String, String> fileOrHeld = prefix -> switch (spec) {
            case ConnectionSpecification.LocalFile f -> "url " + prefix + f.path();
            case ConnectionSpecification.InMemory m -> "held";
            default -> refused;
        };
        Function<String, String> server = prefix -> spec instanceof ConnectionSpecification.StaticDatasource s
                ? "url " + prefix + s.host() + ":" + s.port() + "/" + s.database() : refused;
        return switch (type) {
            case DuckDB -> fileOrHeld.apply("jdbc:duckdb:");
            case SQLite -> fileOrHeld.apply("jdbc:sqlite:");
            case H2 -> switch (spec) {
                case ConnectionSpecification.LocalFile f -> "url jdbc:h2:file:" + f.path();
                case ConnectionSpecification.StaticDatasource s -> "url jdbc:h2:tcp://" + s.host() + ":" + s.port()
                        + "/" + s.database();
                case ConnectionSpecification.EmbeddedH2 e -> "named " + e.databaseName();
                case ConnectionSpecification.LocalH2 l -> "named null";
                case ConnectionSpecification.InMemory m -> "named null";
                default -> refused;
            };
            case Postgres -> server.apply("jdbc:postgresql://");
            case MariaDB, MySQL -> server.apply("jdbc:mariadb://");
            // since 59b547a (the plan path program's step 9, 2026-10-19): a Trino specification opens by its URL, and a
            // session names a user, which a connection of no authentication names none
            case Trino -> spec instanceof ConnectionSpecification.Trino
                    ? "refused: a Trino session names a user, and a connection of no authentication names none"
                            + " (connection '" + Q + "')"
                    : refused;
            // since 59b547a (docs/MONGODB_PLAN_2026_10_16.md D12, 2026-10-17): its connector opens its own sessions,
            // and no JDBC opening reaches it
            case MongoDB -> "refused: connection '" + Q + "' declares MongoDB, which no JDBC opening reaches: its"
                    + " connector opens it, through the plan runner";
            default -> "refused: connections to database type '" + type + "' are not implemented (connection '" + Q
                    + "')";
        };
    }

    @Test
    void eachTypeOpensEachSpecificationAsBefore() {
        for (DatabaseType type : DatabaseType.values()) {
            for (ConnectionSpecification spec : SPECIFICATIONS.values()) {
                ConnectionDefinition def = new ConnectionDefinition(Q, null, type, spec,
                        new AuthenticationSpec.NoAuth());
                assertEquals(expected(type, spec), opening(def), type + " " + spec);
            }
        }
    }

    /** A Trino connection opens at legend-engine's URL (TrinoManager.buildURL: the catalog, then the schema), as the
     *  user the test authentications name, its prepared statements sent as one (the plan path program's step 9,
     *  2026-10-19; moved into the connector at main's merge, 2026-10-22). */
    @Test
    void aTrinoConnectionOpensAtTheEnginesUrl() {
        ConnectionDefinition def = new ConnectionDefinition(Q, null, DatabaseType.Trino,
                new ConnectionSpecification.Trino("trino.example", 8080, "memory", "default", null, false),
                new AuthenticationSpec.TestAuth());
        assertEquals("url jdbc:trino://trino.example:8080/memory/default?user=sa&explicitPrepare=false", opening(def));
        ConnectionDefinition tls = new ConnectionDefinition(Q, null, DatabaseType.Trino,
                new ConnectionSpecification.Trino("trino.example", 443, null, null, null, true),
                new AuthenticationSpec.TestAuth());
        assertEquals("refused: a Trino connection over TLS is not implemented (connection '" + Q + "')", opening(tls));
    }

    /** Whether a database runs in this process is one fact on both sides: the plan side's (its dialect's, which a plan
     *  writes the system metamodel store's tables by) and the execution side's (its connector's, which opens its
     *  private instance) -- the merge of main, 2026-10-22: the branch's {@code Databases.inProcess} became the
     *  dialect's, as main's Phase 5 made every plan fact. */
    @Test
    void aDatabaseRunsInProcessOnBothSidesAlike() {
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            assertEquals(Connectors.of(type).inProcess(), com.legend.database.Databases.dialect(type).inProcess(),
                    type.toString());
        }
    }

    /** A credential is refused by name before the specification is read, on every type. */
    @Test
    void aCredentialIsRefusedFirst() {
        AUTHENTICATIONS.forEach((auth, free) -> {
            assertEquals(free, auth.credentialFree(), auth.toString());
            for (DatabaseType type : DatabaseType.values()) {
                ConnectionDefinition def = new ConnectionDefinition(Q, null, type,
                        new ConnectionSpecification.InMemory(), auth);
                if (free) {
                    assertEquals(expected(type, def.specification()), opening(def), type + " " + auth);
                } else {
                    assertEquals("refused: " + auth.getClass().getSimpleName()
                            + " authentication is not implemented (connection '" + Q + "')", opening(def));
                }
            }
        });
    }

    @Test
    void eachTypesProductAndInstance() throws Exception {
        Map<DatabaseType, String> products = new LinkedHashMap<>();
        products.put(DatabaseType.DuckDB, "DuckDB");
        products.put(DatabaseType.H2, "H2");
        products.put(DatabaseType.Postgres, "PostgreSQL");
        products.put(DatabaseType.SQLite, "SQLite");
        products.put(DatabaseType.Databricks, "SparkSQL");
        products.put(DatabaseType.Snowflake, "Snowflake");
        products.put(DatabaseType.BigQuery, "Google BigQuery");
        products.put(DatabaseType.MariaDB, "MariaDB");
        products.put(DatabaseType.MySQL, "MySQL");
        products.put(DatabaseType.Trino, "Trino");
        products.put(DatabaseType.ClickHouse, "ClickHouse");
        Set<DatabaseType> inProcess = Set.of(DatabaseType.H2, DatabaseType.DuckDB, DatabaseType.SQLite);
        for (DatabaseType type : DatabaseType.values()) {
            String product = products.get(type);
            if (Connectors.of(type).ownSessions()) {
                // MongoDB (since 59b547a, D12 and D14): no JDBC product; a JDBC session handed for it is refused
                NotImplementedException refused = assertThrows(NotImplementedException.class,
                        () -> Sessions.jdbcProduct(type));
                assertEquals("sessions on MongoDB are not JDBC: its connector opens them, through the plan runner",
                        refused.getMessage());
            } else if (product == null) {
                NotImplementedException refused = assertThrows(NotImplementedException.class,
                        () -> Sessions.jdbcProduct(type));
                assertEquals("sessions on database type '" + type + "' are not implemented", refused.getMessage());
            } else {
                assertEquals(product, Sessions.jdbcProduct(type));
            }
            assertEquals(inProcess.contains(type), Sessions.inProcess(type), type.toString());
            if (inProcess.contains(type)) {
                // its private instance is a session of its own database, whose errors carry their raise: no
                // recovery declared, so the seam reads nothing back after a failure (JdbcStatements.held)
                try (Connection c = Connectors.of(type).openPrivate()) {
                    assertSame(Connectors.of(type), Connectors.of(c));
                    assertNull(Connectors.of(type).raiseRecovery());
                    assertNull(JdbcStatements.held(c));
                }
            } else {
                NotImplementedException refused = assertThrows(NotImplementedException.class,
                        () -> Sessions.openPrivate(type));
                assertEquals("no in-memory instance of database type '" + type + "'", refused.getMessage());
            }
        }
    }

    @Test
    void eachTypesStorelessDeclaration() {
        Map<DatabaseType, String> declared = Map.of(
                DatabaseType.DuckDB, "specification: DuckDB { };\n  auth: Test",
                DatabaseType.H2, "specification: LocalH2 { };\n  auth: DefaultH2",
                DatabaseType.SQLite, "specification: SQLite { };\n  auth: Test");
        Set<DatabaseType> servers = Set.of(DatabaseType.Postgres, DatabaseType.MariaDB, DatabaseType.MySQL);
        for (DatabaseType type : DatabaseType.values()) {
            if (declared.containsKey(type)) {
                assertEquals(declared.get(type), Connectors.of(type).storelessDeclaration());
                continue;
            }
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> Connectors.of(type).storelessDeclaration());
            assertEquals(servers.contains(type)
                    ? type + " is a server: its storeless runtime declares the session's coordinates (onServer)"
                    // MongoDB (since 59b547a): a server whose connection is the engine's MongoDBConnection
                    : Connectors.of(type).ownSessions() ? type + " is a server, and its connection is the engine's"
                            + " MongoDBConnection: its storeless runtime declares the session's coordinates (onMongo)"
                    : "no storeless runtime on " + type + ": the platform does not execute on it", refused.getMessage());
        }
    }

    /** The four driver facts each connector answers ({@code DatabaseConnector.decoding}; D72, 2026-10-30, the facts
     *  the dialect's four hooks held until then, carried: the D72 audit asked that they be pinned, since nothing had
     *  pinned the hooks). Positional, so a swapped argument shows here. The eleven JDBC connectors; MongoDB's
     *  statements read no JDBC row. */
    @Test
    void eachConnectorAnswersItsDriversDecodeFacts() {
        DatabaseConnector.Decoding dflt = new DatabaseConnector.Decoding(true, false, false, false);
        Map<DatabaseType, DatabaseConnector.Decoding> expected = new LinkedHashMap<>();
        for (DatabaseType t : List.of(DatabaseType.H2, DatabaseType.SQLite, DatabaseType.Postgres, DatabaseType.MariaDB,
                DatabaseType.MySQL, DatabaseType.ClickHouse)) {
            expected.put(t, dflt);
        }
        // no java.time re-fetch (the drivers refuse getObject(int, LocalDateTime.class); a BC date-time is designed)
        expected.put(DatabaseType.Databricks, new DatabaseConnector.Decoding(false, false, false, false));
        expected.put(DatabaseType.Snowflake, new DatabaseConnector.Decoding(false, false, false, false));
        // a decimal's trailing zeros dropped by the driver (BIGNUMERIC '3.0' reads 3), padded back to the slot's scale
        expected.put(DatabaseType.BigQuery, new DatabaseConnector.Decoding(true, true, false, false));
        // JSON spells a non-finite double as a bare token
        expected.put(DatabaseType.DuckDB, new DatabaseConnector.Decoding(true, false, false, true));
        // a failed statement's result opens with no columns, its error on the first read
        expected.put(DatabaseType.Trino, new DatabaseConnector.Decoding(true, false, true, false));
        for (Map.Entry<DatabaseType, DatabaseConnector.Decoding> e : expected.entrySet()) {
            assertEquals(e.getValue(), Connectors.of(e.getKey()).decoding(), e.getKey().toString());
        }
    }
}
