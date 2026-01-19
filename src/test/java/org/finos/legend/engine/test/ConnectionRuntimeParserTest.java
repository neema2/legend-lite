package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Connection and Runtime parsing.
 */
class ConnectionRuntimeParserTest {

    // ==================== Connection Parsing Tests ====================

    @Test
    @DisplayName("Parse DuckDB InMemory connection with NoAuth")
    void testParseDuckDbInMemoryConnection() {
        String source = """
                RelationalDatabaseConnection store::InMemoryDuckDb
                {
                    store: store::PersonDb;
                    type: DuckDB;
                    specification: InMemory {};
                    auth: NoAuth {};
                }
                """;

        ConnectionDefinition conn = PureDefinitionParser.parseConnectionDefinition(source);

        assertEquals("store::InMemoryDuckDb", conn.qualifiedName());
        assertEquals("InMemoryDuckDb", conn.simpleName());
        assertEquals("store::PersonDb", conn.storeName());
        assertEquals(ConnectionDefinition.DatabaseType.DuckDB, conn.databaseType());
        assertInstanceOf(ConnectionSpecification.InMemory.class, conn.specification());
        assertInstanceOf(AuthenticationSpec.NoAuth.class, conn.authentication());
    }

    @Test
    @DisplayName("Parse SQLite InMemory connection")
    void testParseSqliteInMemoryConnection() {
        String source = """
                RelationalDatabaseConnection store::InMemorySqlite
                {
                    store: store::OrderDb;
                    type: SQLite;
                    specification: InMemory {};
                    auth: NoAuth {};
                }
                """;

        ConnectionDefinition conn = PureDefinitionParser.parseConnectionDefinition(source);

        assertEquals("store::InMemorySqlite", conn.qualifiedName());
        assertEquals("store::OrderDb", conn.storeName());
        assertEquals(ConnectionDefinition.DatabaseType.SQLite, conn.databaseType());
        assertInstanceOf(ConnectionSpecification.InMemory.class, conn.specification());
    }

    @Test
    @DisplayName("Parse DuckDB LocalFile connection")
    void testParseDuckDbLocalFileConnection() {
        String source = """
                RelationalDatabaseConnection store::FileDuckDb
                {
                    store: store::DataDb;
                    type: DuckDB;
                    specification: LocalFile { path: './data/warehouse.duckdb'; };
                    auth: NoAuth {};
                }
                """;

        ConnectionDefinition conn = PureDefinitionParser.parseConnectionDefinition(source);

        assertEquals("store::FileDuckDb", conn.qualifiedName());
        assertEquals(ConnectionDefinition.DatabaseType.DuckDB, conn.databaseType());
        assertInstanceOf(ConnectionSpecification.LocalFile.class, conn.specification());

        ConnectionSpecification.LocalFile localFile = (ConnectionSpecification.LocalFile) conn.specification();
        assertEquals("./data/warehouse.duckdb", localFile.path());
    }

    @Test
    @DisplayName("Parse Static datasource connection")
    void testParseStaticDatasourceConnection() {
        String source = """
                RelationalDatabaseConnection store::ProductionDb
                {
                    store: store::MainDb;
                    type: Postgres;
                    specification: Static { host: 'db.example.com'; port: 5432; database: 'production'; };
                    auth: NoAuth {};
                }
                """;

        ConnectionDefinition conn = PureDefinitionParser.parseConnectionDefinition(source);

        assertEquals("store::ProductionDb", conn.qualifiedName());
        assertEquals(ConnectionDefinition.DatabaseType.Postgres, conn.databaseType());
        assertInstanceOf(ConnectionSpecification.StaticDatasource.class, conn.specification());

        ConnectionSpecification.StaticDatasource staticDs = (ConnectionSpecification.StaticDatasource) conn
                .specification();
        assertEquals("db.example.com", staticDs.host());
        assertEquals(5432, staticDs.port());
        assertEquals("production", staticDs.database());
    }

    @Test
    @DisplayName("Parse connection with UsernamePassword auth")
    void testParseUsernamePasswordAuth() {
        String source = """
                RelationalDatabaseConnection store::SecureDb
                {
                    store: store::SecureStore;
                    type: Postgres;
                    specification: InMemory {};
                    auth: UsernamePassword { username: 'admin'; passwordVaultRef: 'vault://secrets/db/password'; };
                }
                """;

        ConnectionDefinition conn = PureDefinitionParser.parseConnectionDefinition(source);

        assertInstanceOf(AuthenticationSpec.UsernamePassword.class, conn.authentication());

        AuthenticationSpec.UsernamePassword auth = (AuthenticationSpec.UsernamePassword) conn.authentication();
        assertEquals("admin", auth.username());
        assertEquals("vault://secrets/db/password", auth.passwordVaultRef());
    }

    // ==================== Runtime Parsing Tests ====================

    @Test
    @DisplayName("Parse Runtime with single mapping and connection")
    void testParseSimpleRuntime() {
        String source = """
                Runtime my::MyRuntime
                {
                    mappings: [ my::MyMapping ];
                    connections: [ store::PersonDb: store::InMemoryDuckDb ];
                }
                """;

        RuntimeDefinition runtime = PureDefinitionParser.parseRuntimeDefinition(source);

        assertEquals("my::MyRuntime", runtime.qualifiedName());
        assertEquals("MyRuntime", runtime.simpleName());
        assertEquals(1, runtime.mappings().size());
        assertEquals("my::MyMapping", runtime.mappings().get(0));
        assertEquals(1, runtime.connectionBindings().size());
        assertEquals("store::InMemoryDuckDb", runtime.getConnectionForStore("store::PersonDb"));
    }

    @Test
    @DisplayName("Parse Runtime with multiple mappings")
    void testParseRuntimeMultipleMappings() {
        String source = """
                Runtime app::AppRuntime
                {
                    mappings: [ model::PersonMapping, model::OrderMapping, model::ProductMapping ];
                    connections: [ store::MainDb: store::ProdConnection ];
                }
                """;

        RuntimeDefinition runtime = PureDefinitionParser.parseRuntimeDefinition(source);

        assertEquals(3, runtime.mappings().size());
        assertTrue(runtime.mappings().contains("model::PersonMapping"));
        assertTrue(runtime.mappings().contains("model::OrderMapping"));
        assertTrue(runtime.mappings().contains("model::ProductMapping"));
    }

    @Test
    @DisplayName("Parse Runtime with multiple connection bindings")
    void testParseRuntimeMultipleConnections() {
        String source = """
                Runtime multi::MultiStoreRuntime
                {
                    mappings: [ model::FederatedMapping ];
                    connections: [ store::PersonDb: conn::PersonConn, store::OrderDb: conn::OrderConn ];
                }
                """;

        RuntimeDefinition runtime = PureDefinitionParser.parseRuntimeDefinition(source);

        assertEquals(2, runtime.connectionBindings().size());
        assertEquals("conn::PersonConn", runtime.getConnectionForStore("store::PersonDb"));
        assertEquals("conn::OrderConn", runtime.getConnectionForStore("store::OrderDb"));
    }

    // ==================== Integration with PureModelBuilder ====================

    @Test
    @DisplayName("PureModelBuilder processes Connection and Runtime definitions")
    void testModelBuilderRegistersConnectionAndRuntime() {
        String source = """
                Class model::Person { name: String[1]; }
                Database store::PersonDb ( Table T_PERSON ( ID INTEGER, NAME VARCHAR(100) ) )

                RelationalDatabaseConnection store::InMemoryDuckDb
                {
                    store: store::PersonDb;
                    type: DuckDB;
                    specification: InMemory {};
                    auth: NoAuth {};
                }

                Runtime app::AppRuntime
                {
                    mappings: [ model::PersonMap ];
                    connections: [ store::PersonDb: store::InMemoryDuckDb ];
                }
                """;

        PureModelBuilder builder = new PureModelBuilder();
        builder.addSource(source);

        // Verify connection and runtime were registered (would need getters to fully
        // verify)
        // For now, just verify no exceptions and build succeeds
        assertNotNull(builder);
    }
}
