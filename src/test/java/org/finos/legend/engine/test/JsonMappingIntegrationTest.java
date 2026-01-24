package org.finos.legend.engine.test;

import org.finos.legend.engine.server.QueryService;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Order/OrderItem mappings with various combinations
 * of scalar columns and JSON-extracted properties.
 * 
 * Tests the expression-based property mapping feature: ->get('key', @Type)
 */
@DisplayName("JSON Mapping Integration Tests")
class JsonMappingIntegrationTest {

    private Connection connection;
    private QueryService queryService = new QueryService();

    // =========================================================================
    // DATABASE DEFINITIONS
    // =========================================================================

    private static final String DATABASE_SCALAR = """
            Database store::OrderDB (
                Table T_ORDERS (
                    ID INTEGER PRIMARY KEY,
                    CUSTOMER_NAME VARCHAR(100) NOT NULL,
                    TOTAL DECIMAL(10,2)
                )
                Table T_ORDER_ITEMS (
                    ID INTEGER PRIMARY KEY,
                    ORDER_ID INTEGER NOT NULL,
                    PRODUCT_NAME VARCHAR(100),
                    QUANTITY INTEGER,
                    PRICE DECIMAL(10,2)
                )
            )
            """;

    private static final String DATABASE_ORDER_JSON = """
            Database store::OrderDB (
                Table T_ORDERS (
                    ID INTEGER PRIMARY KEY,
                    DATA JSON
                )
                Table T_ORDER_ITEMS (
                    ID INTEGER PRIMARY KEY,
                    ORDER_ID INTEGER NOT NULL,
                    PRODUCT_NAME VARCHAR(100),
                    QUANTITY INTEGER,
                    PRICE DECIMAL(10,2)
                )
            )
            """;

    private static final String DATABASE_ITEM_JSON = """
            Database store::OrderDB (
                Table T_ORDERS (
                    ID INTEGER PRIMARY KEY,
                    CUSTOMER_NAME VARCHAR(100) NOT NULL,
                    TOTAL DECIMAL(10,2)
                )
                Table T_ORDER_ITEMS (
                    ID INTEGER PRIMARY KEY,
                    ORDER_ID INTEGER NOT NULL,
                    DATA JSON
                )
            )
            """;

    private static final String DATABASE_BOTH_JSON = """
            Database store::OrderDB (
                Table T_ORDERS (
                    ID INTEGER PRIMARY KEY,
                    DATA JSON
                )
                Table T_ORDER_ITEMS (
                    ID INTEGER PRIMARY KEY,
                    ORDER_ID INTEGER NOT NULL,
                    DATA JSON
                )
            )
            """;

    private static final String CONNECTION = """
            RelationalDatabaseConnection store::TestConnection {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }
            """;

    private static final String RUNTIME = """
            Runtime test::TestRuntime {
                mappings: [ model::OrderMapping ];
                connections: [ store::OrderDB: store::TestConnection ];
            }
            """;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // =========================================================================
    // TEST 1: Pure scalar columns (baseline)
    // =========================================================================

    @Nested
    @DisplayName("Scalar Columns Only")
    class ScalarColumnsTests {

        @BeforeEach
        void setupScalarTables() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE T_ORDERS (
                            ID INTEGER PRIMARY KEY,
                            CUSTOMER_NAME VARCHAR(100) NOT NULL,
                            TOTAL DECIMAL(10,2)
                        )
                        """);
                stmt.execute("""
                        CREATE TABLE T_ORDER_ITEMS (
                            ID INTEGER PRIMARY KEY,
                            ORDER_ID INTEGER NOT NULL,
                            PRODUCT_NAME VARCHAR(100),
                            QUANTITY INTEGER,
                            PRICE DECIMAL(10,2)
                        )
                        """);
                stmt.execute("INSERT INTO T_ORDERS VALUES (1, 'Alice', 150.00)");
                stmt.execute("INSERT INTO T_ORDERS VALUES (2, 'Bob', 75.50)");
                stmt.execute("INSERT INTO T_ORDER_ITEMS VALUES (1, 1, 'Widget', 2, 50.00)");
                stmt.execute("INSERT INTO T_ORDER_ITEMS VALUES (2, 1, 'Gadget', 1, 50.00)");
                stmt.execute("INSERT INTO T_ORDER_ITEMS VALUES (3, 2, 'Doohickey', 3, 25.17)");
            }
        }

        @Test
        @DisplayName("Order with scalar properties projects correctly")
        void testOrderScalarProjection() throws SQLException {
            String pureModel = """
                    Class model::Order {
                        id: Integer[1];
                        customerName: String[1];
                        total: Float[1];
                    }
                    """ + DATABASE_SCALAR + """
                    Mapping model::OrderMapping (
                        Order: Relational {
                            ~mainTable [OrderDB] T_ORDERS
                            id: [OrderDB] T_ORDERS.ID,
                            customerName: [OrderDB] T_ORDERS.CUSTOMER_NAME,
                            total: [OrderDB] T_ORDERS.TOTAL
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            String query = "Order.all()->project([o | $o.id, o | $o.customerName], ['id', 'name'])";
            var result = queryService.execute(pureModel, query, "test::TestRuntime", connection);

            assertEquals(2, result.rows().size());
            var row1 = result.rows().stream()
                    .filter(r -> ((Number) r.get(0)).intValue() == 1)
                    .findFirst().orElseThrow();
            assertEquals("Alice", row1.get(1));
        }

        @Test
        @DisplayName("OrderItem with scalar properties projects correctly")
        void testOrderItemScalarProjection() throws SQLException {
            String pureModel = """
                    Class model::OrderItem {
                        id: Integer[1];
                        orderId: Integer[1];
                        productName: String[1];
                        quantity: Integer[1];
                        price: Float[1];
                    }
                    """ + DATABASE_SCALAR + """
                    Mapping model::OrderMapping (
                        OrderItem: Relational {
                            ~mainTable [OrderDB] T_ORDER_ITEMS
                            id: [OrderDB] T_ORDER_ITEMS.ID,
                            orderId: [OrderDB] T_ORDER_ITEMS.ORDER_ID,
                            productName: [OrderDB] T_ORDER_ITEMS.PRODUCT_NAME,
                            quantity: [OrderDB] T_ORDER_ITEMS.QUANTITY,
                            price: [OrderDB] T_ORDER_ITEMS.PRICE
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            String query = "OrderItem.all()->project([i | $i.productName, i | $i.quantity], ['product', 'qty'])";
            var result = queryService.execute(pureModel, query, "test::TestRuntime", connection);

            assertEquals(3, result.rows().size());
        }
    }

    // =========================================================================
    // TEST 2: Order JSON, Items Scalar
    // =========================================================================

    @Nested
    @DisplayName("Order JSON + Items Scalar")
    class OrderJsonItemsScalarTests {

        @BeforeEach
        void setupTables() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE T_ORDERS (
                            ID INTEGER PRIMARY KEY,
                            DATA JSON
                        )
                        """);
                stmt.execute("""
                        CREATE TABLE T_ORDER_ITEMS (
                            ID INTEGER PRIMARY KEY,
                            ORDER_ID INTEGER NOT NULL,
                            PRODUCT_NAME VARCHAR(100),
                            QUANTITY INTEGER,
                            PRICE DECIMAL(10,2)
                        )
                        """);
                stmt.execute("INSERT INTO T_ORDERS VALUES (1, '{\"customerName\": \"Alice\", \"total\": 150.00}')");
                stmt.execute("INSERT INTO T_ORDERS VALUES (2, '{\"customerName\": \"Bob\", \"total\": 75.50}')");
                stmt.execute("INSERT INTO T_ORDER_ITEMS VALUES (1, 1, 'Widget', 2, 50.00)");
                stmt.execute("INSERT INTO T_ORDER_ITEMS VALUES (2, 1, 'Gadget', 1, 50.00)");
            }
        }

        @Test
        @DisplayName("Order with JSON-extracted properties")
        void testOrderJsonProperties() throws SQLException {
            String pureModel = """
                    Class model::Order {
                        id: Integer[1];
                        customerName: String[1];
                        total: Float[1];
                    }
                    """ + DATABASE_ORDER_JSON + """
                    Mapping model::OrderMapping (
                        Order: Relational {
                            ~mainTable [OrderDB] T_ORDERS
                            id: [OrderDB] T_ORDERS.ID,
                            customerName: [OrderDB] T_ORDERS.DATA->get('customerName', @String),
                            total: [OrderDB] T_ORDERS.DATA->get('total', @Float)
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            String query = "Order.all()->project([o | $o.id, o | $o.customerName, o | $o.total], ['id', 'name', 'total'])";

            var plan = queryService.compile(pureModel, query, "test::TestRuntime");
            String sql = plan.sqlByStore().values().iterator().next().sql();
            System.out.println("Order JSON SQL: " + sql);

            assertTrue(sql.contains("CAST"), "SQL should contain CAST for JSON extraction");
            assertTrue(sql.contains("customerName") || sql.contains("'customerName'"),
                    "SQL should extract customerName from JSON");

            var result = queryService.execute(pureModel, query, "test::TestRuntime", connection);
            assertEquals(2, result.rows().size());

            var aliceRow = result.rows().stream()
                    .filter(r -> ((Number) r.get(0)).intValue() == 1)
                    .findFirst().orElseThrow();
            assertEquals("Alice", aliceRow.get(1));
        }

        @Test
        @DisplayName("Mixed: Order ID scalar + name from JSON")
        void testMixedOrderProperties() throws SQLException {
            String pureModel = """
                    Class model::Order {
                        id: Integer[1];
                        customerName: String[1];
                    }
                    """ + DATABASE_ORDER_JSON + """
                    Mapping model::OrderMapping (
                        Order: Relational {
                            ~mainTable [OrderDB] T_ORDERS
                            id: [OrderDB] T_ORDERS.ID,
                            customerName: [OrderDB] T_ORDERS.DATA->get('customerName', @String)
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            String query = "Order.all()->filter(o | $o.id == 2)->project([o | $o.customerName], ['name'])";
            var result = queryService.execute(pureModel, query, "test::TestRuntime", connection);

            assertEquals(1, result.rows().size());
            assertEquals("Bob", result.rows().get(0).get(0));
        }
    }

    // =========================================================================
    // TEST 3: Order Scalar, Items JSON
    // =========================================================================

    @Nested
    @DisplayName("Order Scalar + Items JSON")
    class OrderScalarItemsJsonTests {

        @BeforeEach
        void setupTables() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE T_ORDERS (
                            ID INTEGER PRIMARY KEY,
                            CUSTOMER_NAME VARCHAR(100) NOT NULL,
                            TOTAL DECIMAL(10,2)
                        )
                        """);
                stmt.execute("""
                        CREATE TABLE T_ORDER_ITEMS (
                            ID INTEGER PRIMARY KEY,
                            ORDER_ID INTEGER NOT NULL,
                            DATA JSON
                        )
                        """);
                stmt.execute("INSERT INTO T_ORDERS VALUES (1, 'Alice', 150.00)");
                stmt.execute(
                        "INSERT INTO T_ORDER_ITEMS VALUES (1, 1, '{\"productName\": \"Widget\", \"quantity\": 2, \"price\": 50.00}')");
                stmt.execute(
                        "INSERT INTO T_ORDER_ITEMS VALUES (2, 1, '{\"productName\": \"Gadget\", \"quantity\": 1, \"price\": 50.00}')");
            }
        }

        @Test
        @DisplayName("OrderItem with JSON-extracted properties")
        void testOrderItemJsonProperties() throws SQLException {
            String pureModel = """
                    Class model::OrderItem {
                        id: Integer[1];
                        orderId: Integer[1];
                        productName: String[1];
                        quantity: Integer[1];
                        price: Float[1];
                    }
                    """ + DATABASE_ITEM_JSON + """
                    Mapping model::OrderMapping (
                        OrderItem: Relational {
                            ~mainTable [OrderDB] T_ORDER_ITEMS
                            id: [OrderDB] T_ORDER_ITEMS.ID,
                            orderId: [OrderDB] T_ORDER_ITEMS.ORDER_ID,
                            productName: [OrderDB] T_ORDER_ITEMS.DATA->get('productName', @String),
                            quantity: [OrderDB] T_ORDER_ITEMS.DATA->get('quantity', @Integer),
                            price: [OrderDB] T_ORDER_ITEMS.DATA->get('price', @Float)
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            String query = "OrderItem.all()->project([i | $i.productName, i | $i.quantity, i | $i.price], ['product', 'qty', 'price'])";

            var plan = queryService.compile(pureModel, query, "test::TestRuntime");
            String sql = plan.sqlByStore().values().iterator().next().sql();
            System.out.println("OrderItem JSON SQL: " + sql);

            var result = queryService.execute(pureModel, query, "test::TestRuntime", connection);
            assertEquals(2, result.rows().size());

            var widgetRow = result.rows().stream()
                    .filter(r -> "Widget".equals(r.get(0)))
                    .findFirst().orElseThrow();
            assertEquals(2, ((Number) widgetRow.get(1)).intValue());
        }

        @Test
        @DisplayName("Filter on scalar column, project JSON properties")
        void testFilterScalarProjectJson() throws SQLException {
            // Note: Filter on JSON-extracted properties requires additional work
            // For now, filter on scalar columns works correctly
            String pureModel = """
                    Class model::OrderItem {
                        id: Integer[1];
                        orderId: Integer[1];
                        quantity: Integer[1];
                    }
                    """ + DATABASE_ITEM_JSON + """
                    Mapping model::OrderMapping (
                        OrderItem: Relational {
                            ~mainTable [OrderDB] T_ORDER_ITEMS
                            id: [OrderDB] T_ORDER_ITEMS.ID,
                            orderId: [OrderDB] T_ORDER_ITEMS.ORDER_ID,
                            quantity: [OrderDB] T_ORDER_ITEMS.DATA->get('quantity', @Integer)
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            // Filter on scalar orderId, project JSON quantity
            String query = "OrderItem.all()->filter(i | $i.orderId == 1)->project([i | $i.id, i | $i.quantity], ['id', 'qty'])";
            var result = queryService.execute(pureModel, query, "test::TestRuntime", connection);

            assertEquals(2, result.rows().size());
        }
    }

    // =========================================================================
    // TEST 4: Both Order and Items JSON
    // =========================================================================

    @Nested
    @DisplayName("Both Order and Items JSON")
    class BothJsonTests {

        @BeforeEach
        void setupTables() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE T_ORDERS (
                            ID INTEGER PRIMARY KEY,
                            DATA JSON
                        )
                        """);
                stmt.execute("""
                        CREATE TABLE T_ORDER_ITEMS (
                            ID INTEGER PRIMARY KEY,
                            ORDER_ID INTEGER NOT NULL,
                            DATA JSON
                        )
                        """);
                stmt.execute(
                        "INSERT INTO T_ORDERS VALUES (1, '{\"customerName\": \"Alice\", \"status\": \"shipped\"}')");
                stmt.execute(
                        "INSERT INTO T_ORDER_ITEMS VALUES (1, 1, '{\"productName\": \"Widget\", \"quantity\": 2}')");
                stmt.execute(
                        "INSERT INTO T_ORDER_ITEMS VALUES (2, 1, '{\"productName\": \"Gadget\", \"quantity\": 1}')");
            }
        }

        @Test
        @DisplayName("Both Order and OrderItem with JSON-extracted properties")
        void testBothJsonProperties() throws SQLException {
            String pureModel = """
                    Class model::Order {
                        id: Integer[1];
                        customerName: String[1];
                        status: String[1];
                    }
                    Class model::OrderItem {
                        id: Integer[1];
                        orderId: Integer[1];
                        productName: String[1];
                        quantity: Integer[1];
                    }
                    """ + DATABASE_BOTH_JSON + """
                    Mapping model::OrderMapping (
                        Order: Relational {
                            ~mainTable [OrderDB] T_ORDERS
                            id: [OrderDB] T_ORDERS.ID,
                            customerName: [OrderDB] T_ORDERS.DATA->get('customerName', @String),
                            status: [OrderDB] T_ORDERS.DATA->get('status', @String)
                        }
                        OrderItem: Relational {
                            ~mainTable [OrderDB] T_ORDER_ITEMS
                            id: [OrderDB] T_ORDER_ITEMS.ID,
                            orderId: [OrderDB] T_ORDER_ITEMS.ORDER_ID,
                            productName: [OrderDB] T_ORDER_ITEMS.DATA->get('productName', @String),
                            quantity: [OrderDB] T_ORDER_ITEMS.DATA->get('quantity', @Integer)
                        }
                    )
                    """ + CONNECTION + RUNTIME;

            // Query orders
            String orderQuery = "Order.all()->project([o | $o.customerName, o | $o.status], ['name', 'status'])";
            var orderResult = queryService.execute(pureModel, orderQuery, "test::TestRuntime", connection);
            assertEquals(1, orderResult.rows().size());
            assertEquals("Alice", orderResult.rows().get(0).get(0));
            assertEquals("shipped", orderResult.rows().get(0).get(1));

            // Query items
            String itemQuery = "OrderItem.all()->project([i | $i.productName, i | $i.quantity], ['product', 'qty'])";
            var itemResult = queryService.execute(pureModel, itemQuery, "test::TestRuntime", connection);
            assertEquals(2, itemResult.rows().size());
        }
    }
}
