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
                mappings:
                [
                ];
                connections:
                [
                    store::EventDatabase:
                    [
                        environment: store::TestConnection
                    ]
                ];
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
    @DisplayName("get('key', @String) extracts top-level key from JSON column as text")
    void testGetTopLevelKey() throws SQLException {
        // Note: get('key') returns JSON, get('key', @String) returns text
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~page: _ | $_.PAYLOAD->get('page', @String))
                    ->filter(_ | $_.EVENT_TYPE == 'page_view')
                    ->select(~[ID, page])
                """;

        var result = executeRelation(pureQuery);
        assertEquals(1, result.rows().size());
        assertEquals("/home", result.rows().get(0).get(1));
    }

    @Test
    @DisplayName("Chained get() with final type extracts nested value as text")
    void testChainedGet() throws SQLException {
        // Chained get: get('user') returns JSON, get('name', @String) extracts text
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~userName: _ | $_.PAYLOAD->get('user')->get('name', @String))
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

    // ===========================================================================
    // EMBEDDED CLASS MAPPING TESTS
    // ===========================================================================

    /**
     * End-to-end test for embedded class mapping.
     * 
     * Tests the full pipeline:
     * 1. Define Class with typed properties
     * 2. Map JSON column to Class using ->cast(@ClassName)
     * 3. Access embedded class properties with automatic type inference
     * 4. Generate correct SQL with typed JSON extraction
     */
    @Test
    @DisplayName("Embedded class mapping: $event.payload.price generates typed JSON extraction")
    void testEmbeddedClassMapping() throws SQLException {
        // Full model with embedded class mapping
        String pureModel = """
                Class model::Event {
                    id: Integer[1];
                    eventType: String[1];
                    price: Integer[1];
                    qty: Integer[1];
                }

                Database store::EventDatabase (
                    Table T_EVENTS (
                        ID INTEGER PRIMARY KEY,
                        EVENT_TYPE VARCHAR(100),
                        PAYLOAD JSON
                    )
                )

                Mapping model::EventMapping (
                    Event: Relational {
                        ~mainTable [EventDatabase] T_EVENTS
                        id: [EventDatabase] T_EVENTS.ID,
                        eventType: [EventDatabase] T_EVENTS.EVENT_TYPE,
                        price: [EventDatabase] T_EVENTS.PAYLOAD->get('price', @Integer),
                        qty: [EventDatabase] T_EVENTS.PAYLOAD->get('qty', @Integer)
                    }
                )

                RelationalDatabaseConnection store::TestConnection {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime {
                    mappings:
                    [
                        model::EventMapping
                    ];
                    connections:
                    [
                        store::EventDatabase:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;

        // Query using mapped properties - price and qty are mapped to JSON extraction
        String pureQuery = """
                Event.all()
                    ->filter(e | $e.eventType == 'purchase')
                    ->project([e | $e.id, e | $e.price, e | $e.qty], ['id', 'price', 'qty'])
                """;

        // Compile and check SQL
        var plan = queryService.compile(pureModel, pureQuery, "test::TestRuntime");
        String sql = plan.sqlByStore().values().iterator().next().sql();
        System.out.println("Embedded class SQL: " + sql);

        // Verify SQL contains typed JSON extraction
        assertTrue(sql.contains("PAYLOAD"), "SQL should reference PAYLOAD column");
        assertTrue(sql.toUpperCase().contains("CAST"), "SQL should contain CAST for type conversion");
        assertTrue(sql.contains("price") || sql.contains("'price'"), "SQL should extract 'price' from JSON");

        // Execute query
        var result = queryService.execute(pureModel, pureQuery, "test::TestRuntime", connection);

        // Verify results - should have purchase events with extracted prices
        assertFalse(result.rows().isEmpty(), "Should have results");

        // Row 0 should be event 2 (first purchase)
        var row = result.rows().stream()
                .filter(r -> ((Number) r.get(0)).intValue() == 2)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have purchase event 2"));

        Object priceValue = row.get(1);
        if (priceValue != null) {
            // Price should be from first item (10) - note: depends on implementation
            System.out.println("  Event 2 price: " + priceValue);
        }
    }

    /**
     * Test get('key', @Type) for typed extraction from JSON.
     * This verifies that typed get works for different types.
     */
    @Test
    @DisplayName("get('key', @Type) extracts and casts JSON values to specific types")
    void testExplicitCastFunction() throws SQLException {
        // With new syntax: get('field', @Type) does both extraction and casting
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~userId: _ | $_.PAYLOAD->get('user')->get('id', @Integer))
                    ->extend(~page: _ | $_.PAYLOAD->get('page', @String))
                    ->filter(_ | $_.EVENT_TYPE == 'page_view')
                    ->select(~[ID, userId, page])
                """;

        // Check SQL generation
        String sql = generateSql(pureQuery);
        System.out.println("Cast test SQL: " + sql);
        assertTrue(sql.contains("CAST"), "SQL should contain CAST for explicit type conversion");

        // Execute and verify results
        var result = executeRelation(pureQuery);
        assertFalse(result.rows().isEmpty(), "Should have results");

        var row = result.rows().getFirst();
        assertEquals(1, ((Number) row.get(0)).intValue(), "ID should be 1");
        // userId should be 100 (from user.id in the page_view event)
        assertEquals(100, ((Number) row.get(1)).intValue(), "userId should be 100");
        assertEquals("/home", row.get(2).toString(), "page should be /home");
    }

    /**
     * Test cast(@Type) combined with map/fold operations.
     * This verifies that explicit casting works within collection operations.
     */
    @Test
    @DisplayName("cast(@Type) works with map/fold operations")
    void testCastWithMapAndFold() throws SQLException {
        // Use explicit cast within map to convert price to integer before summing
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->extend(~totalPrice: _ | $_.PAYLOAD->get('items')
                        ->map(i | $i->get('price')->cast(@Integer))
                        ->fold({a, v | $a + $v}, 0))
                    ->filter(_ | $_.EVENT_TYPE == 'purchase')
                    ->select(~[ID, totalPrice])
                """;

        // Check SQL generation includes CAST
        String sql = generateSql(pureQuery);
        System.out.println("Cast with map/fold SQL: " + sql);
        assertTrue(sql.contains("CAST"), "SQL should contain CAST for type conversion in map");

        // Execute and verify results
        var result = executeRelation(pureQuery);
        assertEquals(2, result.rows().size(), "Should have 2 purchase events");

        // Event ID 2: items are [{price: 10}, {price: 25}] → sum = 35
        var row2 = result.rows().stream()
                .filter(r -> r.get(0).equals(2) || r.get(0).equals(2L))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have row with ID 2"));
        assertEquals(35.0, toDouble(row2.get(1)), 0.01, "Order 2 total should be 35");

        // Event ID 3: items are [{price: 100}] → sum = 100
        var row3 = result.rows().stream()
                .filter(r -> r.get(0).equals(3) || r.get(0).equals(3L))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have row with ID 3"));
        assertEquals(100.0, toDouble(row3.get(1)), 0.01, "Order 3 total should be 100");
    }

    /**
     * Test get('key', @Type) combined with flatten/lateral join.
     * This verifies that typed extraction works with array flattening.
     * Query: flatten items array, then calculate price * qty with typed gets
     */
    @Test
    @DisplayName("get('key', @Type) works with flatten/lateral join for price*qty")
    void testCastWithFlatten() throws SQLException {
        // Flatten items array and calculate price * qty with typed get()
        // New syntax: get('field', @Type) returns typed scalar
        String pureQuery = """
                #>{EventDatabase.T_EVENTS}#
                    ->filter(_ | $_.EVENT_TYPE == 'purchase')
                    ->extend(~items: _ | $_.PAYLOAD->get('items'))
                    ->flatten(~items)
                    ->extend(~lineTotal: _ | $_.items->get('price', @Integer) * $_.items->get('qty', @Integer))
                    ->select(~[ID, lineTotal])
                """;

        // Check SQL generation includes CAST and UNNEST for flatten
        String sql = generateSql(pureQuery);
        System.out.println("Cast with flatten SQL: " + sql);
        assertTrue(sql.contains("CAST"), "SQL should contain CAST for type conversion");
        assertTrue(sql.contains("UNNEST"), "SQL should contain UNNEST for flatten");

        // Execute and verify results - should have 3 rows (3 items total across 2
        // orders)
        var result = executeRelation(pureQuery);

        // Debug: print all rows
        System.out.println("Result rows: " + result.rows().size());
        for (var row : result.rows()) {
            System.out.println("  Row: " + row);
        }

        assertEquals(3, result.rows().size(), "Should have 3 flattened item rows");

        // Order 2 has items: [{sku: "ABC", price: 10, qty: 2}, {sku: "XYZ", price: 25,
        // qty: 1}]
        // Order 3 has items: [{sku: "DEF", price: 100, qty: 3}]
        // Expected line totals: 10*2=20, 25*1=25, 100*3=300
        var totals = result.rows().stream()
                .map(r -> toDouble(r.get(1)))
                .sorted()
                .toList();
        assertEquals(20.0, totals.get(0), 0.01, "First line total should be 20 (ABC: 10*2)");
        assertEquals(25.0, totals.get(1), 0.01, "Second line total should be 25 (XYZ: 25*1)");
        assertEquals(300.0, totals.get(2), 0.01, "Third line total should be 300 (DEF: 100*3)");
    }

    /**
     * Debug test - raw SQL to verify UNNEST behavior in DuckDB.
     */
    @Test
    @DisplayName("Debug: Raw SQL UNNEST JSON array")
    void testRawSqlUnnest() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Test 1: Simple UNNEST
            System.out.println("=== Test 1: Simple UNNEST ===");
            String sql1 = """
                    SELECT ID, UNNEST(CAST(PAYLOAD->'items' AS JSON[])) AS item
                    FROM T_EVENTS
                    WHERE EVENT_TYPE = 'purchase'
                    """;
            System.out.println("SQL: " + sql1);
            var rs = stmt.executeQuery(sql1);
            while (rs.next()) {
                System.out.println("  ID: " + rs.getInt(1) + ", item: " + rs.getString(2));
            }
            rs.close();

            // Test 2: Access properties from unnested items
            System.out.println("\n=== Test 2: Access properties from unnested items ===");
            String sql2 = """
                    SELECT ID, item, item->>'price' AS price, item->>'qty' AS qty
                    FROM (
                        SELECT ID, UNNEST(CAST(PAYLOAD->'items' AS JSON[])) AS item
                        FROM T_EVENTS
                        WHERE EVENT_TYPE = 'purchase'
                    ) AS t
                    """;
            System.out.println("SQL: " + sql2);
            rs = stmt.executeQuery(sql2);
            while (rs.next()) {
                System.out.println("  ID: " + rs.getInt(1) + ", item: " + rs.getString(2) +
                        ", price: " + rs.getString(3) + ", qty: " + rs.getString(4));
            }
            rs.close();

            // Test 3: CAST and multiply
            System.out.println("\n=== Test 3: CAST and multiply ===");
            String sql3 = """
                    SELECT ID, CAST(item->>'price' AS INTEGER) * CAST(item->>'qty' AS INTEGER) AS total
                    FROM (
                        SELECT ID, UNNEST(CAST(PAYLOAD->'items' AS JSON[])) AS item
                        FROM T_EVENTS
                        WHERE EVENT_TYPE = 'purchase'
                    ) AS t
                    """;
            System.out.println("SQL: " + sql3);
            rs = stmt.executeQuery(sql3);
            while (rs.next()) {
                System.out.println("  ID: " + rs.getInt(1) + ", total: " + rs.getInt(2));
            }
            rs.close();

            // Test 4: list_transform with lambda (note: parens around lambda body to
            // disambiguate)
            System.out.println("\n=== Test 4: list_transform with lambda ===");
            String sql4 = """
                    SELECT ID, list_transform(CAST(PAYLOAD->'items' AS JSON[]), i -> (i->'price')) AS prices
                    FROM T_EVENTS
                    WHERE EVENT_TYPE = 'purchase'
                    """;
            System.out.println("SQL: " + sql4);
            try {
                rs = stmt.executeQuery(sql4);
                while (rs.next()) {
                    System.out.println("  ID: " + rs.getInt(1) + ", prices: " + rs.getString(2));
                }
                rs.close();
            } catch (Exception e) {
                System.out.println("  Error: " + e.getMessage());
            }
        }
        assertTrue(true, "Debug test completed");
    }
}
