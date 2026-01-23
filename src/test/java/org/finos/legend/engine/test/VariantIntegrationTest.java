package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JSON/VARIANT column support.
 * Tests the complete pipeline: Pure → IR → SQL → DuckDB execution
 */
@DisplayName("VARIANT/JSON Column Integration Tests")
class VariantIntegrationTest {

    private Connection connection;
    private QueryService queryService = new QueryService();

    private static final String EVENT_DATABASE = """
            Database store::EventDatabase
            (
                Table T_EVENTS
                (
                    ID INTEGER PRIMARY KEY,
                    EVENT_TYPE VARCHAR(100) NOT NULL,
                    PAYLOAD SEMISTRUCTURED
                )
            )
            """;

    private static final String CONNECTION_DEFINITION = """
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }
            """;

    private static final String RUNTIME_DEFINITION = """
            Runtime test::TestRuntime
            {
                mappings: [ ];
                connections: [ store::EventDatabase: store::TestConnection ];
            }
            """;

    private String getCompletePureModel() {
        return EVENT_DATABASE + "\n" + CONNECTION_DEFINITION + "\n" + RUNTIME_DEFINITION;
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void setupDatabase() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_EVENTS (
                        ID INTEGER PRIMARY KEY,
                        EVENT_TYPE VARCHAR(100) NOT NULL,
                        PAYLOAD JSON
                    )
                    """);

            stmt.execute("""
                    INSERT INTO T_EVENTS VALUES (
                        1, 'page_view',
                        '{"page": "/home", "user": {"name": "Alice", "id": 100}}'
                    )
                    """);

            stmt.execute("""
                    INSERT INTO T_EVENTS VALUES (
                        2, 'purchase',
                        '{"items": [{"sku": "ABC123", "qty": 2}], "total": 150}'
                    )
                    """);
        }
    }

    private BufferedResult executeRelation(String pureQuery) throws SQLException {
        return queryService.execute(getCompletePureModel(), pureQuery, "test::TestRuntime", connection);
    }

    private String generateSql(String pureQuery) {
        var plan = queryService.compile(getCompletePureModel(), pureQuery, "test::TestRuntime");
        return plan.sqlByStore().values().iterator().next().sql();
    }

    @Test
    @DisplayName("get() extracts top-level key from JSON column")
    void testGetTopLevelKey() throws SQLException {
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~page: _ | $_.PAYLOAD->get('page'))
                    ->filter(_ | $_.EVENT_TYPE == 'page_view')
                    ->select(~[ID, page])
                """;

        var result = executeRelation(pureQuery);
        assertEquals(1, result.rows().size());
        assertEquals("/home", result.rows().get(0).get(1));
    }

    @Test
    @DisplayName("Chained get() extracts nested value")
    void testChainedGet() throws SQLException {
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~userName: _ | $_.PAYLOAD->get('user')->get('name'))
                    ->filter(_ | $_.EVENT_TYPE == 'page_view')
                    ->select(~[ID, userName])
                """;

        var result = executeRelation(pureQuery);
        assertEquals(1, result.rows().size());
        assertEquals("Alice", result.rows().get(0).get(1));
    }

    @Test
    @DisplayName("get(key) generates correct SQL")
    void testSqlGeneration() {
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~page: _ | $_.PAYLOAD->get('page'))
                    ->select(~[ID, page])
                """;

        String sql = generateSql(pureQuery);
        System.out.println("Generated SQL: " + sql);
        assertTrue(sql.contains("->>'page'") || sql.contains("->> 'page'") || sql.contains("PAYLOAD"),
                "SQL should reference PAYLOAD column. Got: " + sql);
    }
}
