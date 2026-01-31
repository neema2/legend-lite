package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.dsl.ModelContext;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * True end-to-end integration tests for user-defined enums.
 * 
 * Tests the complete flow:
 * 1. Define enum in Pure source
 * 2. Store in registry via PureModelBuilder
 * 3. Execute queries through QueryService
 * 4. Filter and project using enum values
 */
@DisplayName("User-Defined Enum Integration Tests")
class EnumIntegrationTest {

    private Connection connection;
    private QueryService queryService;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        queryService = new QueryService();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ==================== End-to-End Enum Tests ====================

    @Nested
    @DisplayName("End-to-End: Enum Definition and Storage")
    class EndToEndTests {

        @Test
        @DisplayName("Pure enum is parsed, stored in registry, and retrievable")
        void testEnumParsedAndStored() {
            // GIVEN: Pure source with enum definition
            String pureSource = """
                    Enum model::OrderStatus
                    {
                        PENDING,
                        CONFIRMED,
                        SHIPPED,
                        DELIVERED,
                        CANCELLED
                    }
                    """;

            // WHEN: We parse and add to builder
            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // THEN: The enum is stored and retrievable by simple name
            EnumDefinition retrieved = builder.getEnum("OrderStatus");
            assertNotNull(retrieved, "Enum should be retrievable by simple name");
            assertEquals("model::OrderStatus", retrieved.qualifiedName());
            assertEquals(5, retrieved.values().size());
            assertTrue(retrieved.hasValue("PENDING"));
            assertTrue(retrieved.hasValue("SHIPPED"));
            assertFalse(retrieved.hasValue("UNKNOWN"));
        }

        @Test
        @DisplayName("Enum retrievable by qualified name")
        void testEnumRetrievableByQualifiedName() {
            // GIVEN: Pure source with enum
            String pureSource = """
                    Enum com::example::Priority { LOW, MEDIUM, HIGH }
                    """;

            // WHEN: We parse
            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // THEN: Retrievable by qualified name
            EnumDefinition retrieved = builder.getEnum("com::example::Priority");
            assertNotNull(retrieved);
            assertEquals("com::example::Priority", retrieved.qualifiedName());
        }

        @Test
        @DisplayName("Multiple enums can be defined and stored")
        void testMultipleEnums() {
            // GIVEN: Pure source with multiple enum definitions
            String pureSource = """
                    Enum model::Priority { LOW, MEDIUM, HIGH, CRITICAL }

                    Enum model::Status { ACTIVE, INACTIVE, PENDING }

                    Enum model::Color { RED, GREEN, BLUE, YELLOW }
                    """;

            // WHEN: We parse
            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // THEN: All enums are stored
            assertNotNull(builder.getEnum("Priority"));
            assertNotNull(builder.getEnum("Status"));
            assertNotNull(builder.getEnum("Color"));

            // AND: Each has correct values
            assertEquals(4, builder.getEnum("Priority").values().size());
            assertEquals(3, builder.getEnum("Status").values().size());
            assertEquals(4, builder.getEnum("Color").values().size());

            // AND: hasEnumValue works correctly
            assertTrue(builder.hasEnumValue("Priority", "CRITICAL"));
            assertTrue(builder.hasEnumValue("Status", "ACTIVE"));
            assertTrue(builder.hasEnumValue("Color", "BLUE"));
            assertFalse(builder.hasEnumValue("Priority", "LOW_PRIORITY"));
        }

        @Test
        @DisplayName("Enum and Class can be defined together")
        void testEnumWithClass() {
            // GIVEN: Full model with enum and class using it
            String pureSource = """
                    Enum model::OrderStatus
                    {
                        PENDING,
                        CONFIRMED,
                        SHIPPED,
                        DELIVERED
                    }

                    Class model::Order
                    {
                        orderId: String[1];
                        status: OrderStatus[1];
                        amount: Float[1];
                    }
                    """;

            // WHEN: We build the model
            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // THEN: Both enum and class are stored
            assertNotNull(builder.getEnum("OrderStatus"));
            assertNotNull(builder.getClass("Order"));

            // AND: The class has the expected properties
            var orderClass = builder.getClass("Order");
            assertNotNull(orderClass.getProperty("status"));
            assertNotNull(orderClass.getProperty("orderId"));

            // AND: The status property has enum type
            assertEquals("OrderStatus", orderClass.getProperty("status").genericType().typeName());
            assertNotNull(orderClass.getProperty("amount"));
        }

        @Test
        @DisplayName("Full model with enum, class, database, and mapping")
        void testCompleteModelWithEnum() {
            // GIVEN: Complete Pure model
            String pureSource = """
                    Enum model::TaskStatus
                    {
                        TODO,
                        IN_PROGRESS,
                        DONE,
                        BLOCKED
                    }

                    Class model::Task
                    {
                        id: Integer[1];
                        title: String[1];
                        status: TaskStatus[1];
                    }

                    Database store::TaskDB
                    (
                        Table TASKS
                        (
                            ID INTEGER PRIMARY KEY,
                            TITLE VARCHAR(200),
                            STATUS VARCHAR(20)
                        )
                    )

                    Mapping model::TaskMapping
                    (
                        Task: Relational
                        {
                            ~mainTable [store::TaskDB] TASKS
                            id: [store::TaskDB]TASKS.ID,
                            title: [store::TaskDB]TASKS.TITLE,
                            status: [store::TaskDB]TASKS.STATUS
                        }
                    )
                    """;

            // WHEN: We build the model
            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // THEN: All elements are stored
            assertNotNull(builder.getEnum("TaskStatus"));
            assertNotNull(builder.getClass("Task"));
            assertNotNull(builder.getTable("TASKS"));
            assertTrue(builder.getMappingRegistry().findByClassName("Task").isPresent());

            // AND: Enum values are validated
            assertTrue(builder.hasEnumValue("TaskStatus", "TODO"));
            assertTrue(builder.hasEnumValue("TaskStatus", "IN_PROGRESS"));
            assertTrue(builder.hasEnumValue("TaskStatus", "DONE"));
            assertTrue(builder.hasEnumValue("TaskStatus", "BLOCKED"));
            assertFalse(builder.hasEnumValue("TaskStatus", "CANCELLED"));
        }

        @Test
        @DisplayName("ModelContext.findEnum interface integration")
        void testModelContextFindEnum() {
            // GIVEN: Pure source with enum
            String pureSource = """
                    Enum model::PaymentMethod
                    {
                        CREDIT_CARD,
                        DEBIT_CARD,
                        BANK_TRANSFER,
                        PAYPAL,
                        CRYPTO
                    }
                    """;

            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // WHEN: We use the ModelContext interface
            ModelContext context = builder;

            // THEN: findEnum works via interface
            var found = context.findEnum("PaymentMethod");
            assertTrue(found.isPresent());
            assertEquals("model::PaymentMethod", found.get().qualifiedName());
            assertEquals(5, found.get().values().size());

            // AND: hasEnumValue works via interface default method
            assertTrue(context.hasEnumValue("PaymentMethod", "PAYPAL"));
            assertTrue(context.hasEnumValue("PaymentMethod", "CRYPTO"));
            assertFalse(context.hasEnumValue("PaymentMethod", "CASH"));
        }

        @Test
        @DisplayName("Enum with single value is valid")
        void testEnumWithSingleValue() {
            // GIVEN: Pure source with single-value enum
            String pureSource = """
                    Enum model::SingletonStatus { ONLY_VALUE }
                    """;

            // WHEN: We parse
            PureModelBuilder builder = new PureModelBuilder();
            builder.addSource(pureSource);

            // THEN: Enum is stored correctly
            EnumDefinition enumDef = builder.getEnum("SingletonStatus");
            assertNotNull(enumDef);
            assertEquals(1, enumDef.values().size());
            assertTrue(enumDef.hasValue("ONLY_VALUE"));
        }
    }

    // ==================== EnumDefinition Unit Tests ====================

    @Nested
    @DisplayName("Unit: EnumDefinition Record")
    class EnumDefinitionTests {

        @Test
        @DisplayName("EnumDefinition.of creates with varargs")
        void testEnumDefinitionOf() {
            EnumDefinition enumDef = EnumDefinition.of("model::Status", "ACTIVE", "INACTIVE");

            assertEquals("model::Status", enumDef.qualifiedName());
            assertEquals("Status", enumDef.simpleName());
            assertEquals("model", enumDef.packagePath());
            assertEquals(2, enumDef.values().size());
            assertTrue(enumDef.hasValue("ACTIVE"));
        }

        @Test
        @DisplayName("EnumDefinition validates at least one value required")
        void testEnumRequiresValues() {
            assertThrows(IllegalArgumentException.class, () -> EnumDefinition.of("model::Empty"));
        }

        @Test
        @DisplayName("EnumDefinition.hasValue is case-sensitive")
        void testEnumValueCaseSensitive() {
            EnumDefinition enumDef = EnumDefinition.of("model::Color", "RED", "GREEN", "BLUE");

            assertTrue(enumDef.hasValue("RED"));
            assertFalse(enumDef.hasValue("red"));
            assertFalse(enumDef.hasValue("Red"));
        }

        @Test
        @DisplayName("EnumDefinition.simpleName extracts from qualified")
        void testSimpleNameExtraction() {
            EnumDefinition short_ = EnumDefinition.of("Status", "A");
            EnumDefinition medium = EnumDefinition.of("model::Status", "A");
            EnumDefinition full = EnumDefinition.of("org::example::model::Status", "A");

            assertEquals("Status", short_.simpleName());
            assertEquals("Status", medium.simpleName());
            assertEquals("Status", full.simpleName());
        }

        @Test
        @DisplayName("EnumDefinition.packagePath extracts correctly")
        void testPackagePathExtraction() {
            EnumDefinition noPackage = EnumDefinition.of("Status", "A");
            EnumDefinition oneLevel = EnumDefinition.of("model::Status", "A");
            EnumDefinition multiLevel = EnumDefinition.of("org::example::model::Status", "A");

            assertEquals("", noPackage.packagePath());
            assertEquals("model", oneLevel.packagePath());
            assertEquals("org::example::model", multiLevel.packagePath());
        }
    }

    // ==================== QueryService Integration Tests ====================

    @Nested
    @DisplayName("QueryService: Full Pipeline Execution")
    class QueryServiceTests {

        @Test
        @DisplayName("Query with enum filter executes through full pipeline")
        void testQueryWithEnumFilter() throws SQLException {
            // GIVEN: Database with enum-like column
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE TASKS (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100),
                            STATUS VARCHAR(20)
                        )
                        """);
                stmt.execute("""
                        INSERT INTO TASKS VALUES
                        (1, 'Fix bug', 'PENDING'),
                        (2, 'Write docs', 'IN_PROGRESS'),
                        (3, 'Deploy', 'DONE'),
                        (4, 'Review PR', 'PENDING')
                        """);
            }

            // AND: Complete Pure model with enum, class, db, mapping, connection, runtime
            String pureSource = """
                    Enum model::TaskStatus
                    {
                        PENDING,
                        IN_PROGRESS,
                        DONE,
                        CANCELLED
                    }

                    Class model::Task
                    {
                        id: Integer[1];
                        name: String[1];
                        status: TaskStatus[1];
                    }

                    Database store::TaskDB
                    (
                        Table TASKS
                        (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100),
                            STATUS VARCHAR(20)
                        )
                    )

                    Mapping model::TaskMapping
                    (
                        Task: Relational
                        {
                            ~mainTable [store::TaskDB] TASKS
                            id: [store::TaskDB]TASKS.ID,
                            name: [store::TaskDB]TASKS.NAME,
                            status: [store::TaskDB]TASKS.STATUS
                        }
                    )

                    RelationalDatabaseConnection store::TaskConnection { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }

                    Runtime app::TaskRuntime { mappings: [ model::TaskMapping ]; connections: [ store::TaskDB: [ environment: store::TaskConnection ] ]; }
                    """;

            // WHEN: We query for PENDING tasks through QueryService
            String pureQuery = """
                    Task.all()
                        ->filter({t | $t.status == 'PENDING'})
                        ->project({t | $t.name})
                    """;

            BufferedResult result = queryService.execute(
                    pureSource, pureQuery, "app::TaskRuntime", connection);

            // THEN: Both PENDING tasks are returned
            assertEquals(2, result.rows().size());
        }

        @Test
        @DisplayName("Query with enum IN clause")
        void testQueryWithEnumInClause() throws SQLException {
            // GIVEN: Database with priority enum
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE ISSUES (
                            ID INTEGER PRIMARY KEY,
                            TITLE VARCHAR(100),
                            PRIORITY VARCHAR(20)
                        )
                        """);
                stmt.execute("""
                        INSERT INTO ISSUES VALUES
                        (1, 'Critical bug', 'CRITICAL'),
                        (2, 'Minor typo', 'LOW'),
                        (3, 'Important feature', 'HIGH'),
                        (4, 'Nice-to-have', 'MEDIUM')
                        """);
            }

            String pureSource = """
                    Enum model::Priority { LOW, MEDIUM, HIGH, CRITICAL }

                    Class model::Issue
                    {
                        id: Integer[1];
                        title: String[1];
                        priority: Priority[1];
                    }

                    Database store::IssueDB
                    (
                        Table ISSUES
                        (
                            ID INTEGER PRIMARY KEY,
                            TITLE VARCHAR(100),
                            PRIORITY VARCHAR(20)
                        )
                    )

                    Mapping model::IssueMapping
                    (
                        Issue: Relational
                        {
                            ~mainTable [store::IssueDB] ISSUES
                            id: [store::IssueDB]ISSUES.ID,
                            title: [store::IssueDB]ISSUES.TITLE,
                            priority: [store::IssueDB]ISSUES.PRIORITY
                        }
                    )

                    RelationalDatabaseConnection store::IssueConnection { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }

                    Runtime app::IssueRuntime { mappings: [ model::IssueMapping ]; connections: [ store::IssueDB: [ environment: store::IssueConnection ] ]; }
                    """;

            // WHEN: Query for HIGH or CRITICAL priority
            String pureQuery = """
                    Issue.all()
                        ->filter({i | $i.priority == 'HIGH' || $i.priority == 'CRITICAL'})
                        ->project({i | $i.title}, {i | $i.priority})
                    """;

            BufferedResult result = queryService.execute(
                    pureSource, pureQuery, "app::IssueRuntime", connection);

            // THEN: Both HIGH and CRITICAL issues are returned
            assertEquals(2, result.rows().size());
        }

        @Test
        @DisplayName("Query projecting enum column")
        void testQueryProjectingEnumColumn() throws SQLException {
            // GIVEN: Database with order status
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                        CREATE TABLE ORDERS (
                            ID INTEGER PRIMARY KEY,
                            CUSTOMER VARCHAR(100),
                            STATUS VARCHAR(20)
                        )
                        """);
                stmt.execute("""
                        INSERT INTO ORDERS VALUES
                        (1, 'Alice', 'SHIPPED'),
                        (2, 'Bob', 'PENDING'),
                        (3, 'Charlie', 'DELIVERED')
                        """);
            }

            String pureSource = """
                    Enum model::OrderStatus { PENDING, SHIPPED, DELIVERED, CANCELLED }

                    Class model::Order
                    {
                        id: Integer[1];
                        customer: String[1];
                        status: OrderStatus[1];
                    }

                    Database store::OrderDB
                    (
                        Table ORDERS
                        (
                            ID INTEGER PRIMARY KEY,
                            CUSTOMER VARCHAR(100),
                            STATUS VARCHAR(20)
                        )
                    )

                    Mapping model::OrderMapping
                    (
                        Order: Relational
                        {
                            ~mainTable [store::OrderDB] ORDERS
                            id: [store::OrderDB]ORDERS.ID,
                            customer: [store::OrderDB]ORDERS.CUSTOMER,
                            status: [store::OrderDB]ORDERS.STATUS
                        }
                    )

                    RelationalDatabaseConnection store::OrderConnection { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }

                    Runtime app::OrderRuntime { mappings: [ model::OrderMapping ]; connections: [ store::OrderDB: [ environment: store::OrderConnection ] ]; }
                    """;

            // WHEN: Query projecting status enum column
            String pureQuery = """
                    Order.all()
                        ->project({o | $o.customer}, {o | $o.status})
                    """;

            BufferedResult result = queryService.execute(
                    pureSource, pureQuery, "app::OrderRuntime", connection);

            // THEN: All orders are returned with their status values
            assertEquals(3, result.rows().size());

            // Verify enum values are returned as strings
            var rows = result.rows();
            assertTrue(rows.stream().anyMatch(r -> "SHIPPED".equals(r.values().get(1))));
            assertTrue(rows.stream().anyMatch(r -> "PENDING".equals(r.values().get(1))));
            assertTrue(rows.stream().anyMatch(r -> "DELIVERED".equals(r.values().get(1))));
        }
    }
}
