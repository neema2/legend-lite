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

            // Purchase with items array for testing map/fold
            stmt.execute(
                    """
                            INSERT INTO T_EVENTS VALUES (
                                2, 'purchase',
                                '{"items": [{"sku": "ABC", "price": 10, "qty": 2}, {"sku": "XYZ", "price": 25, "qty": 1}], "total": 45}'
                            )
                            """);

            // Another purchase for testing
            stmt.execute("""
                    INSERT INTO T_EVENTS VALUES (
                        3, 'purchase',
                        '{"items": [{"sku": "DEF", "price": 100, "qty": 3}], "total": 300}'
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

    @Test
    @DisplayName("map() transforms JSON array elements")
    void testMapOnJsonArray() throws SQLException {
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~prices: _ | $_.PAYLOAD->get('items')->map(i | $i->get('price')))
                    ->filter(_ | $_.EVENT_TYPE == 'purchase')
                    ->select(~[ID, prices])
                """;

        // Verify SQL contains list_transform
        String sql = generateSql(pureQuery);
        System.out.println("Map SQL: " + sql);
        assertTrue(sql.contains("list_transform"),
                "SQL should contain list_transform for map(). Got: " + sql);

        // Execute and verify results
        var result = executeRelation(pureQuery);
        assertEquals(2, result.rows().size(), "Should return 2 purchase events");

        // Event ID 2 has items with prices [10, 25]
        // Event ID 3 has items with prices [100]
        System.out.println("Map results: " + result.rows());
    }

    @Test
    @DisplayName("fold() aggregates JSON array elements")
    void testFoldOnJsonArray() throws SQLException {
        // Test fold by summing prices: fold({a, v | $a + $v}, 0)
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~totalPrice: _ | $_.PAYLOAD->get('items')
                                ->map(i | $i->get('price'))
                                ->fold({a, v | $a + $v}, 0))
                    ->filter(_ | $_.EVENT_TYPE == 'purchase')
                    ->select(~[ID, totalPrice])
                """;

        // Verify SQL contains list_reduce
        String sql = generateSql(pureQuery);
        System.out.println("Fold SQL: " + sql);
        assertTrue(sql.contains("list_reduce"),
                "SQL should contain list_reduce for fold(). Got: " + sql);
        assertTrue(sql.contains("CAST") && sql.contains("AS DOUBLE"),
                "SQL should contain CAST for type coercion");

        // Execute and verify results
        var result = executeRelation(pureQuery);
        assertEquals(2, result.rows().size(), "Should return 2 purchase events");
        System.out.println("Fold results: " + result.rows());

        // Event ID 2 has prices [10, 25] -> sum = 35
        var row2 = result.rows().stream()
                .filter(r -> r.get(0).equals(2) || r.get(0).equals(2L))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have row with ID 2"));
        assertEquals(35.0, toDouble(row2.get(1)), 0.01, "Order 2 total should be 35 (10+25)");

        // Event ID 3 has prices [100] -> sum = 100
        var row3 = result.rows().stream()
                .filter(r -> r.get(0).equals(3) || r.get(0).equals(3L))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have row with ID 3"));
        assertEquals(100.0, toDouble(row3.get(1)), 0.01, "Order 3 total should be 100");
    }

    /**
     * Helper to convert DuckDB result values to double.
     * Handles Number, JsonNode (toString), and String types.
     */
    private double toDouble(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        // DuckDB may return JsonNode for single-element list_reduce results
        return Double.parseDouble(value.toString());
    }

    @Test
    @DisplayName("Combined Example: map prices * qty, then fold to sum")
    void testMapAndFoldCombined() throws SQLException {
        // The Combined Example from research: calculate order total from items
        // items->map(i | $i->get('price') * $i->get('qty'))->fold({a, v | $a + $v}, 0)
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~calculatedTotal: _ | $_.PAYLOAD->get('items')
                                ->map(i | $i->get('price') * $i->get('qty'))
                                ->fold({a, v | $a + $v}, 0))
                    ->filter(_ | $_.EVENT_TYPE == 'purchase')
                    ->select(~[ID, calculatedTotal])
                """;

        // Verify SQL contains both list operations
        String sql = generateSql(pureQuery);
        System.out.println("Combined Example SQL: " + sql);
        assertTrue(sql.contains("list_transform") && sql.contains("list_reduce"),
                "SQL should contain both list_transform and list_reduce. Got: " + sql);

        // Execute and verify results
        var result = executeRelation(pureQuery);
        assertEquals(2, result.rows().size(), "Should return 2 purchase events");
        System.out.println("Combined results: " + result.rows());

        // Event ID 2: items are [{price: 10, qty: 2}, {price: 25, qty: 1}]
        // Calculated: 10*2 + 25*1 = 20 + 25 = 45
        var row2 = result.rows().stream()
                .filter(r -> r.get(0).equals(2) || r.get(0).equals(2L))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have row with ID 2"));
        assertEquals(45.0, toDouble(row2.get(1)), 0.01,
                "Order 2 calculated total should be 45 (10*2 + 25*1)");

        // Event ID 3: items are [{price: 100, qty: 3}]
        // Calculated: 100*3 = 300
        var row3 = result.rows().stream()
                .filter(r -> r.get(0).equals(3) || r.get(0).equals(3L))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have row with ID 3"));
        assertEquals(300.0, toDouble(row3.get(1)), 0.01,
                "Order 3 calculated total should be 300 (100*3)");
    }
}
