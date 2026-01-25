package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for class inheritance support.
 * 
 * Tests the full pipeline: Parse → Compile → SQL Generate → Execute
 * for queries that access inherited properties.
 */
@DisplayName("Class Inheritance Tests")
class InheritanceIntegrationTest {

    /**
     * Tests that classes with extends clause can be queried and inherited
     * properties
     * are accessible through the full query pipeline.
     */
    @Nested
    @DisplayName("End-to-End Inheritance Tests")
    class EndToEndTests {

        private Connection connection;
        private QueryService queryService;

        /**
         * Complete Pure model with inheritance hierarchy:
         * Entity → Person → Employee
         * 
         * Person inherits 'id' from Entity
         * Employee inherits 'id', 'firstName', 'lastName' from Person
         */
        private static final String INHERITANCE_MODEL = """
                Class model::Entity
                {
                    id: String[1];
                }

                Class model::Person extends model::Entity
                {
                    firstName: String[1];
                    lastName: String[1];
                }

                Class model::Employee extends model::Person
                {
                    employeeId: String[1];
                    department: String[1];
                    salary: Integer[1];
                }

                Database store::EmployeeDatabase
                (
                    Table T_EMPLOYEE
                    (
                        ID VARCHAR(50) PRIMARY KEY,
                        FIRST_NAME VARCHAR(100) NOT NULL,
                        LAST_NAME VARCHAR(100) NOT NULL,
                        EMPLOYEE_ID VARCHAR(50) NOT NULL,
                        DEPARTMENT VARCHAR(50) NOT NULL,
                        SALARY INTEGER NOT NULL
                    )
                )

                Mapping model::EmployeeMapping
                (
                    Employee: Relational
                    {
                        ~mainTable [EmployeeDatabase] T_EMPLOYEE
                        id: [EmployeeDatabase] T_EMPLOYEE.ID,
                        firstName: [EmployeeDatabase] T_EMPLOYEE.FIRST_NAME,
                        lastName: [EmployeeDatabase] T_EMPLOYEE.LAST_NAME,
                        employeeId: [EmployeeDatabase] T_EMPLOYEE.EMPLOYEE_ID,
                        department: [EmployeeDatabase] T_EMPLOYEE.DEPARTMENT,
                        salary: [EmployeeDatabase] T_EMPLOYEE.SALARY
                    }
                )

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime
                {
                    mappings: [ model::EmployeeMapping ];
                    connections: [
                        store::EmployeeDatabase: [
                            conn1: store::TestConnection
                        ]
                    ];
                }
                """;

        @BeforeEach
        void setUp() throws Exception {
            connection = DriverManager.getConnection("jdbc:duckdb:");
            queryService = new QueryService();

            // Create table with all columns (including inherited)
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                            CREATE TABLE T_EMPLOYEE (
                                ID VARCHAR(50) PRIMARY KEY,
                                FIRST_NAME VARCHAR(100),
                                LAST_NAME VARCHAR(100),
                                EMPLOYEE_ID VARCHAR(50),
                                DEPARTMENT VARCHAR(50),
                                SALARY INTEGER
                            )
                        """);

                // Insert test data
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES ('E001', 'John', 'Doe', 'EMP001', 'Engineering', 100000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES ('E002', 'Jane', 'Smith', 'EMP002', 'Engineering', 95000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES ('E003', 'Bob', 'Johnson', 'EMP003', 'Sales', 85000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES ('E004', 'Alice', 'Williams', 'EMP004', 'Sales', 80000)");
            }
        }

        @AfterEach
        void tearDown() throws Exception {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }

        private BufferedResult executeQuery(String pureQuery) throws SQLException {
            return queryService.execute(
                    INHERITANCE_MODEL,
                    pureQuery,
                    "test::TestRuntime",
                    connection);
        }

        private String generateSql(String pureQuery) {
            var plan = queryService.compile(
                    INHERITANCE_MODEL,
                    pureQuery,
                    "test::TestRuntime");
            return plan.sqlByStore().values().iterator().next().sql();
        }

        // ==================== Filter on Inherited Property ====================

        @Test
        @DisplayName("Filter on inherited property (firstName from Person)")
        void testFilterOnInheritedProperty() throws Exception {
            // Use curly brace lambda syntax: {p | $p.firstName == 'John'}
            String pureQuery = """
                    Employee.all()
                        ->filter({e | $e.firstName == 'John'})
                        ->project({e | $e.firstName}, {e | $e.lastName}, {e | $e.employeeId})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            assertEquals(1, result.rowCount(), "Should find 1 employee named John");

            int firstNameIdx = findColumnIndex(result, "firstName");
            int lastNameIdx = findColumnIndex(result, "lastName");

            assertEquals("John", result.rows().get(0).get(firstNameIdx));
            assertEquals("Doe", result.rows().get(0).get(lastNameIdx));
        }

        @Test
        @DisplayName("Filter on grandparent inherited property (id from Entity)")
        void testFilterOnGrandparentProperty() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->filter({e | $e.id == 'E001'})
                        ->project({e | $e.id}, {e | $e.firstName}, {e | $e.employeeId})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            assertEquals(1, result.rowCount(), "Should find 1 employee with id E001");

            int idIdx = findColumnIndex(result, "id");
            int firstNameIdx = findColumnIndex(result, "firstName");

            assertEquals("E001", result.rows().get(0).get(idIdx));
            assertEquals("John", result.rows().get(0).get(firstNameIdx));
        }

        // ==================== Project Inherited Properties ====================

        @Test
        @DisplayName("Project mix of own and inherited properties")
        void testProjectMixedProperties() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.id}, {e | $e.firstName}, {e | $e.lastName}, {e | $e.employeeId}, {e | $e.department})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            assertEquals(4, result.rowCount(), "Should retrieve all 4 employees");
            assertEquals(5, result.columns().size(), "Should have 5 projected columns");

            // Verify column names exist (mix of own and inherited)
            assertTrue(result.columns().stream().anyMatch(c -> c.name().equals("id")));
            assertTrue(result.columns().stream().anyMatch(c -> c.name().equals("firstName")));
            assertTrue(result.columns().stream().anyMatch(c -> c.name().equals("lastName")));
            assertTrue(result.columns().stream().anyMatch(c -> c.name().equals("employeeId")));
            assertTrue(result.columns().stream().anyMatch(c -> c.name().equals("department")));
        }

        // ==================== Combined Filter + Sort on Inherited ====================

        @Test
        @DisplayName("Filter and sort on inherited properties")
        void testFilterAndSortOnInheritedProperties() throws Exception {
            // Filter before project for class-based filtering
            String pureQuery = """
                    Employee.all()
                        ->filter({e | $e.salary > 82000})
                        ->project({e | $e.firstName}, {e | $e.lastName}, {e | $e.salary})
                        ->sort({r | $r.lastName})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            assertEquals(3, result.rowCount(), "Should find 3 employees with salary > 82000");

            // Verify sorted by lastName
            int lastNameIdx = findColumnIndex(result, "lastName");
            String firstLastName = (String) result.rows().get(0).get(lastNameIdx);
            String secondLastName = (String) result.rows().get(1).get(lastNameIdx);

            assertTrue(firstLastName.compareTo(secondLastName) <= 0,
                    "Results should be sorted by lastName");
        }

        // ==================== SQL Generation Verification ====================

        @Test
        @DisplayName("Generated SQL contains correct column mappings for inherited properties")
        void testSqlGenerationForInheritedProperties() {
            String pureQuery = """
                    Employee.all()
                        ->filter({e | $e.firstName == 'John'})
                        ->project({e | $e.firstName}, {e | $e.employeeId})
                    """;

            String sql = generateSql(pureQuery);

            // Verify SQL references the correct physical column for inherited property
            assertTrue(sql.contains("FIRST_NAME"),
                    "SQL should reference FIRST_NAME column for inherited firstName property");
            assertTrue(sql.contains("John"),
                    "SQL should contain filter value");
        }

        private int findColumnIndex(BufferedResult result, String columnName) {
            for (int i = 0; i < result.columns().size(); i++) {
                if (result.columns().get(i).name().equals(columnName)) {
                    return i;
                }
            }
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
    }
}
