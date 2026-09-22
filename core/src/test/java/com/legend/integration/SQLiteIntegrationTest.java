package com.legend.integration;

import com.legend.exec.ExecutionResult;
import com.legend.compiler.element.ModelContext;
import com.legend.model.ClassDefinition;
import com.legend.model.DatabaseDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests using SQLite as the execution engine.
 * 
 * Demonstrates full Pure syntax for:
 * - Class definitions:
 * {@code Class package::Name { property: Type[multiplicity]; }}
 * - Database definitions: {@code Database package::Name ( Table ... )}
 * - Mapping definitions:
 * {@code Mapping package::Name ( ClassName: Relational { ... } )}
 * - Query expressions: {@code ClassName.all()->filter({p | ...})->project(...)}
 */
@DisplayName("SQLite Integration Tests - Full Pure Language")
class SQLiteIntegrationTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() {
        return "SQLite";
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:sqlite::memory:"; // In-memory SQLite
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ==================== Pure Definition Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure Class with optional property")
    void testParseClassWithOptionalProperty() {
        // GIVEN: A Pure Class with optional and many multiplicity
        String pureClass = """
                ###Pure
                import model::*;

                Class model::Employee
                {
                    name: String[1];
                    email: String[0..1];
                    phoneNumbers: String[*];
                }
                """;

        // WHEN: We parse it with core
        ClassDefinition classDef = parseOne(pureClass, ClassDefinition.class);

        // THEN: We get correct multiplicities (flat bounds; null upper = *)
        assertEquals("model::Employee", classDef.qualifiedName());
        assertEquals(3, classDef.properties().size());

        var name = (com.legend.protocol.Multiplicity.Concrete)
                classDef.properties().get(0).multiplicity();
        assertEquals(1, name.lowerBound());
        assertEquals(1, name.upperBound());

        var email = (com.legend.protocol.Multiplicity.Concrete)
                classDef.properties().get(1).multiplicity();
        assertEquals(0, email.lowerBound());
        assertEquals(1, email.upperBound());

        var phones = (com.legend.protocol.Multiplicity.Concrete)
                classDef.properties().get(2).multiplicity();
        assertEquals(0, phones.lowerBound());
        assertNull(phones.upperBound());
    }

    @Test
    @DisplayName("Parse Pure Database with multiple tables")
    void testParseDatabaseWithMultipleTables() {
        // GIVEN: A Pure Database with multiple tables
        String pureDatabase = """
                ###Relational
                Database store::SalesDB
                (
                    Table T_CUSTOMER
                    (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(200) NOT NULL
                    )
                    Table T_ORDER
                    (
                        ID INTEGER PRIMARY KEY,
                        CUSTOMER_ID INTEGER NOT NULL,
                        TOTAL DECIMAL(10,2) NOT NULL
                    )
                )
                """;

        // WHEN: We parse it with core
        DatabaseDefinition dbDef = parseOne(pureDatabase, DatabaseDefinition.class);

        // THEN: We get both tables
        assertEquals("store::SalesDB", dbDef.qualifiedName());
        assertEquals(2, dbDef.tables().size());

        assertTrue(table(dbDef, "T_CUSTOMER") != null);
        assertTrue(table(dbDef, "T_ORDER") != null);

        var orderTable = table(dbDef, "T_ORDER");
        assertEquals(3, orderTable.columns().size());
    }

    @Test
    @DisplayName("Compiled core model has runtime objects from Pure")
    void testModelBuilderFromPure() {
        // GIVEN: The complete Pure model, compiled by core
        ModelContext ctx = com.legend.Compiler.compileModel(COMPLETE_PURE_MODEL);

        assertTrue(ctx.findClass("model::Person").isPresent());

        var personTable = ctx.findTableDefinition("store::PersonDatabase", "T_PERSON")
                .orElseThrow();
        assertEquals(5, personTable.columns().size());

        var mapping = ctx.findLegacyMapping("model::PersonMapping").orElseThrow()
                .classMappings().stream()
                .filter(cm -> cm instanceof com.legend.model.ClassMapping.Relational r
                        && r.className().equals("model::Person"))
                .map(cm -> (com.legend.model.ClassMapping.Relational) cm)
                .findFirst().orElseThrow();
        assertEquals(3, mapping.propertyMappings().size());
    }

    // ==================== Pure Language Query Tests ====================

    @Test
    @DisplayName("Pure: model::Person.all()->filter({p | $p.lastName == 'Smith'})->project(...)")
    void testPureFindSmithsQuery() throws SQLException {
        // GIVEN: A Pure query to find all Smiths
        // Note: Pure lambdas use curly braces: {param | body}
        String pureQuery = """
                model::Person.all()
                    ->filter({p | $p.lastName == 'Smith'})
                    ->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName])
                """;

        // WHEN: We compile and execute the Pure query
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: We find the 2 Smiths
        assertEquals(2, results.size(), "Should find 2 Smiths");
        assertTrue(results.stream().anyMatch(p -> "John".equals(p.firstName())));
        assertTrue(results.stream().anyMatch(p -> "Jane".equals(p.firstName())));
    }

    @Test
    @DisplayName("Pure: Complex filter with AND")
    void testPureComplexFilterWithAnd() throws SQLException {
        // GIVEN: A Pure query with AND condition
        String pureQuery = """
                model::Person.all()
                    ->filter({p | $p.lastName == 'Smith' && ($p.age > 25)})
                    ->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName, age:p|$p.age])
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: Both John (30) and Jane (28) are over 25
        assertEquals(2, results.size());
        for (PersonResult person : results) {
            assertEquals("Smith", person.lastName());
            assertTrue(person.age() > 25);
        }
    }

    @Test
    @DisplayName("Pure: Filter with OR condition")
    void testPureFilterWithOr() throws SQLException {
        // GIVEN: A Pure query with OR condition
        String pureQuery = """
                model::Person.all()
                    ->filter({p | $p.lastName == 'Smith' || $p.lastName == 'Jones'})
                    ->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName])
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: All 3 people match
        assertEquals(3, results.size());
    }

    @Test
    @DisplayName("Pure: Get all people with all fields")
    void testPureGetAllWithAllFields() throws SQLException {
        // GIVEN: A Pure query to get all people
        String pureQuery = """
                model::Person.all()
                    ->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName, age:p|$p.age])
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: All 3 people are returned
        assertEquals(3, results.size());

        // Verify all data
        assertTrue(results.stream()
                .anyMatch(p -> "John".equals(p.firstName()) && "Smith".equals(p.lastName()) && p.age() == 30));
        assertTrue(results.stream()
                .anyMatch(p -> "Jane".equals(p.firstName()) && "Smith".equals(p.lastName()) && p.age() == 28));
        assertTrue(results.stream()
                .anyMatch(p -> "Bob".equals(p.firstName()) && "Jones".equals(p.lastName()) && p.age() == 45));
    }

    @Test
    @DisplayName("Pure: Filter by integer comparison")
    void testPureFilterByIntegerComparison() throws SQLException {
        // GIVEN: Various integer comparison queries

        // age < 30
        String queryLessThan = "model::Person.all()->filter({p | $p.age < 30})->project(~[firstName:p|$p.firstName])";
        assertEquals(1, executePureQuery(queryLessThan).size()); // Jane only

        // age <= 30
        String queryLessOrEqual = "model::Person.all()->filter({p | $p.age <= 30})->project(~[firstName:p|$p.firstName])";
        assertEquals(2, executePureQuery(queryLessOrEqual).size()); // Jane and John

        // age > 30
        String queryGreaterThan = "model::Person.all()->filter({p | $p.age > 30})->project(~[firstName:p|$p.firstName])";
        assertEquals(1, executePureQuery(queryGreaterThan).size()); // Bob only

        // age >= 30
        String queryGreaterOrEqual = "model::Person.all()->filter({p | $p.age >= 30})->project(~[firstName:p|$p.firstName])";
        assertEquals(2, executePureQuery(queryGreaterOrEqual).size()); // John and Bob
    }

    // ==================== SQLite-Specific Tests ====================

    // ==================== Cross-Dialect Compatibility ====================

    @Test
    @DisplayName("Pure query with NOT EQUALS operator")
    void testPureNotEquals() throws SQLException {
        // GIVEN: A Pure query with != operator
        String pureQuery = """
                model::Person.all()
                    ->filter({p | $p.lastName != 'Smith'})
                    ->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName])
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: Only Bob Jones is returned
        assertEquals(1, results.size());
        assertEquals("Bob", results.getFirst().firstName());
        assertEquals("Jones", results.getFirst().lastName());
    }

    // ==================== Function Definition Tests ====================

    @Test
    @DisplayName("Function with Class query - execute body against SQLite")
    void testFunctionWithClassQuery_SQLite() throws Exception {
        // GIVEN: A model with a function, connection and runtime
        String pureSource = """
                ###Pure
                import model::*;
                import store::*;
                import test::*;

                Class model::Adult { name: String[1]; age: Integer[1]; }
                ###Relational
                Database store::AdultDb ( Table T_ADULT ( ID INTEGER, NAME VARCHAR(100), AGE INTEGER ) )
                ###Mapping
                import model::*;
                import store::*;
                import test::*;
                Mapping model::AdultMap ( Adult: Relational { ~mainTable [AdultDb] T_ADULT name: [AdultDb] T_ADULT.NAME, age: [AdultDb] T_ADULT.AGE } )

                ###Pure
                import model::*;
                import store::*;
                import test::*;
                function query::getAdults(): model::Adult[*]
                {
                    model::Adult.all()->filter({p | $p.age >= 18})
                }

                ###Connection
                import model::*;
                import store::*;
                import test::*;
                RelationalDatabaseConnection store::TestConnection
                {
                    type: SQLite;
                    specification: SQLite { };
                    auth: Test;
                }

                ###Runtime
                import model::*;
                import store::*;
                import test::*;
                Runtime test::TestRuntime
                {
                    mappings:
                    [
                        model::AdultMap
                    ];
                    connections:
                    [
                        store::AdultDb:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;

        // Setup test data
        connection.createStatement().execute(
                "CREATE TABLE T_ADULT (ID INTEGER, NAME VARCHAR(100), AGE INTEGER)");
        connection.createStatement().execute(
                "INSERT INTO T_ADULT VALUES (1, 'Alice', 25), (2, 'Bob', 15), (3, 'Charlie', 30)");

        // Execute the function body via QueryService
        String functionBody = "model::Adult.all()->filter({p | $p.age >= 18})";
        var result = queryService.execute(pureSource, functionBody, "test::TestRuntime", connection);

        // THEN: Bare class query → JSON-wrapped GraphResult
        assertInstanceOf(ExecutionResult.Graph.class, result);
        String json = result.asGraph().json();
        assertNotNull(json);
        assertTrue(json.contains("Alice"), "JSON should contain Alice (age 25)");
        assertTrue(json.contains("Charlie"), "JSON should contain Charlie (age 30)");
        assertFalse(json.contains("Bob"), "JSON should NOT contain Bob (age 15, filtered out)");
    }

    @Test
    @DisplayName("Function with Relation query - execute body against SQLite")
    void testFunctionWithRelationQuery_SQLite() throws Exception {
        // GIVEN: A model with a function, connection and runtime
        String pureSource = """
                ###Pure
                import model::*;
                import store::*;
                import test::*;

                Class model::Worker { dept: String[1]; salary: Integer[1]; }
                ###Relational
                Database store::WorkerDb ( Table T_WORKER ( ID INTEGER, DEPT VARCHAR(50), SALARY INTEGER ) )
                ###Mapping
                import model::*;
                import store::*;
                import test::*;
                Mapping model::WorkerMap ( Worker: Relational { ~mainTable [WorkerDb] T_WORKER dept: [WorkerDb] T_WORKER.DEPT, salary: [WorkerDb] T_WORKER.SALARY } )

                ###Pure
                import model::*;
                import store::*;
                import test::*;
                function query::getWorkerInfo(): Any[*]
                {
                    model::Worker.all()->project([{w | $w.dept}, {w | $w.salary}], ['department', 'sal'])
                }

                ###Connection
                import model::*;
                import store::*;
                import test::*;
                RelationalDatabaseConnection store::TestConnection
                {
                    type: SQLite;
                    specification: SQLite { };
                    auth: Test;
                }

                ###Runtime
                import model::*;
                import store::*;
                import test::*;
                Runtime test::TestRuntime
                {
                    mappings:
                    [
                        model::WorkerMap
                    ];
                    connections:
                    [
                        store::WorkerDb:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;

        // Setup test data
        connection.createStatement().execute(
                "CREATE TABLE T_WORKER (ID INTEGER, DEPT VARCHAR(50), SALARY INTEGER)");
        connection.createStatement().execute(
                "INSERT INTO T_WORKER VALUES (1, 'Engineering', 100000), (2, 'Engineering', 120000), (3, 'Sales', 80000)");

        // Execute the function body via QueryService
        String functionBody = "model::Worker.all()->project([{w | $w.dept}, {w | $w.salary}], ['department', 'sal'])";
        var result = queryService.execute(pureSource, functionBody, "test::TestRuntime", connection);

        // THEN: Should return 3 rows with projected columns
        assertEquals(3, result.rows().size(), "Should have 3 worker rows");
    }

    /** Parse one element of the given kind with core's parser. */
    private static <T> T parseOne(String source, Class<T> kind) {
        return com.legend.testing.Own.model(source).elements().stream()
                .filter(kind::isInstance).map(kind::cast)
                .findFirst().orElseThrow();
    }

    private static DatabaseDefinition.@com.legend.base.Nullable TableDefinition table(
            DatabaseDefinition db, String name) {
        return db.tables().stream()
                .filter(t -> t.name().equals(name)).findFirst().orElse(null);
    }
}
