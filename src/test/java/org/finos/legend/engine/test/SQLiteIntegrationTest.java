package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.m3.*;

import org.junit.jupiter.api.*;

import java.sql.*;
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
    protected SQLDialect getDialect() {
        return SQLiteDialect.INSTANCE;
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:sqlite::memory:"; // In-memory SQLite
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        sqlGenerator = new SQLGenerator(getDialect());
        setupDatabase();
        setupMappingRegistry();
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
                Class model::Employee
                {
                    name: String[1];
                    email: String[0..1];
                    phoneNumbers: String[*];
                }
                """;

        // WHEN: We parse it
        ClassDefinition classDef = PureDefinitionParser.parseClassDefinition(pureClass);

        // THEN: We get correct multiplicities
        assertEquals("model::Employee", classDef.qualifiedName());
        assertEquals(3, classDef.properties().size());

        var name = classDef.properties().get(0);
        assertEquals("1", name.multiplicityString());

        var email = classDef.properties().get(1);
        assertEquals("0..1", email.multiplicityString());

        var phones = classDef.properties().get(2);
        assertEquals("*", phones.multiplicityString());
    }

    @Test
    @DisplayName("Parse Pure Database with multiple tables")
    void testParseDatabaseWithMultipleTables() {
        // GIVEN: A Pure Database with multiple tables
        String pureDatabase = """
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
                        TOTAL DECIMAL NOT NULL
                    )
                )
                """;

        // WHEN: We parse it
        DatabaseDefinition dbDef = PureDefinitionParser.parseDatabaseDefinition(pureDatabase);

        // THEN: We get both tables
        assertEquals("store::SalesDB", dbDef.qualifiedName());
        assertEquals(2, dbDef.tables().size());

        assertTrue(dbDef.findTable("T_CUSTOMER").isPresent());
        assertTrue(dbDef.findTable("T_ORDER").isPresent());

        var orderTable = dbDef.findTable("T_ORDER").orElseThrow();
        assertEquals(3, orderTable.columns().size());
    }

    @Test
    @DisplayName("Model builder creates runtime objects from Pure")
    void testModelBuilderFromPure() {
        // GIVEN: The complete Pure model

        // THEN: Model builder has all objects
        assertNotNull(modelBuilder);

        PureClass personClass = modelBuilder.getClass("Person");
        assertNotNull(personClass);
        assertEquals("Person", personClass.name());

        Table personTable = modelBuilder.getTable("T_PERSON");
        assertNotNull(personTable);
        assertEquals(4, personTable.columns().size());

        RelationalMapping mapping = mappingRegistry.getByClassName("Person");
        assertEquals(3, mapping.propertyMappings().size());
    }

    // ==================== Pure Language Query Tests ====================

    @Test
    @DisplayName("Pure: Person.all()->filter({p | $p.lastName == 'Smith'})->project(...)")
    void testPureFindSmithsQuery() throws SQLException {
        // GIVEN: A Pure query to find all Smiths
        // Note: Pure lambdas use curly braces: {param | body}
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.lastName == 'Smith'})
                    ->project({p | $p.firstName}, {p | $p.lastName})
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
                Person.all()
                    ->filter({p | $p.lastName == 'Smith' && $p.age > 25})
                    ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age})
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
                Person.all()
                    ->filter({p | $p.lastName == 'Smith' || $p.lastName == 'Jones'})
                    ->project({p | $p.firstName}, {p | $p.lastName})
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
                Person.all()
                    ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age})
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
        String queryLessThan = "Person.all()->filter({p | $p.age < 30})->project({p | $p.firstName})";
        assertEquals(1, executePureQuery(queryLessThan).size()); // Jane only

        // age <= 30
        String queryLessOrEqual = "Person.all()->filter({p | $p.age <= 30})->project({p | $p.firstName})";
        assertEquals(2, executePureQuery(queryLessOrEqual).size()); // Jane and John

        // age > 30
        String queryGreaterThan = "Person.all()->filter({p | $p.age > 30})->project({p | $p.firstName})";
        assertEquals(1, executePureQuery(queryGreaterThan).size()); // Bob only

        // age >= 30
        String queryGreaterOrEqual = "Person.all()->filter({p | $p.age >= 30})->project({p | $p.firstName})";
        assertEquals(2, executePureQuery(queryGreaterOrEqual).size()); // John and Bob
    }

    // ==================== SQLite-Specific Tests ====================

    @Test
    @DisplayName("Boolean handling in SQLite (uses 1/0)")
    void testBooleanHandling() {
        SQLiteDialect dialect = SQLiteDialect.INSTANCE;

        // SQLite uses 1/0 for booleans
        assertEquals("1", dialect.formatBoolean(true));
        assertEquals("0", dialect.formatBoolean(false));
    }

    // ==================== Cross-Dialect Compatibility ====================

    @Test
    @DisplayName("SQL generated is compatible across dialects for standard queries")
    void testCrossDialectCompatibility() {
        // GIVEN: Same Pure query
        String pureQuery = "Person.all()->filter({p | $p.lastName == 'Smith'})->project({p | $p.firstName}, {p | $p.lastName})";

        // WHEN: We generate SQL with both dialects
        SQLGenerator duckdbGen = new SQLGenerator(DuckDBDialect.INSTANCE);
        SQLGenerator sqliteGen = new SQLGenerator(SQLiteDialect.INSTANCE);

        RelationNode plan = pureCompiler.compile(pureQuery);

        String duckdbSql = duckdbGen.generate(plan);
        String sqliteSql = sqliteGen.generate(plan);

        // THEN: For standard queries, they should be identical
        assertEquals(duckdbSql, sqliteSql,
                "Standard queries should generate identical SQL");
    }

    @Test
    @DisplayName("Pure query with NOT EQUALS operator")
    void testPureNotEquals() throws SQLException {
        // GIVEN: A Pure query with != operator
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.lastName != 'Smith'})
                    ->project({p | $p.firstName}, {p | $p.lastName})
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
        // GIVEN: A model with a function that returns a filtered Class query
        String model = """
                Class model::Adult { name: String[1]; age: Integer[1]; }
                Database store::AdultDb ( Table T_ADULT ( ID INTEGER, NAME VARCHAR(100), AGE INTEGER ) )
                Mapping model::AdultMap ( Adult: Relational { ~mainTable [AdultDb] T_ADULT name: [AdultDb] T_ADULT.NAME, age: [AdultDb] T_ADULT.AGE } )

                function query::getAdults(): model::Adult[*]
                {
                    Adult.all()->filter({p | $p.age >= 18})
                }
                """;

        // Setup test data (unique table name to avoid conflicts with setUp)
        connection.createStatement().execute(
                "CREATE TABLE T_ADULT (ID INTEGER, NAME VARCHAR(100), AGE INTEGER)");
        connection.createStatement().execute(
                "INSERT INTO T_ADULT VALUES (1, 'Alice', 25), (2, 'Bob', 15), (3, 'Charlie', 30)");

        // Parse the model (includes the function definition)
        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(model);
        MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
        pureCompiler = new org.finos.legend.pure.dsl.PureCompiler(mappingRegistry);

        // WHEN: Parse and compile the function BODY directly
        String functionBody = "Adult.all()->filter({p | $p.age >= 18})";
        RelationNode plan = pureCompiler.compile(functionBody);
        String sql = sqlGenerator.generate(plan);

        System.out.println("SQLite Function body SQL (getAdults): " + sql);

        // Execute against SQLite
        ResultSet rs = connection.createStatement().executeQuery(sql);

        // THEN: Should return only adults (age >= 18)
        int count = 0;
        while (rs.next()) {
            count++;
            int age = rs.getInt("AGE");
            assertTrue(age >= 18, "Should only return adults, got age: " + age);
        }
        assertEquals(2, count, "Should return 2 adults (Alice and Charlie)");
    }

    @Test
    @DisplayName("Function with Relation query - execute body against SQLite")
    void testFunctionWithRelationQuery_SQLite() throws Exception {
        // GIVEN: A model with a function that returns a projected query
        String model = """
                Class model::Worker { dept: String[1]; salary: Integer[1]; }
                Database store::WorkerDb ( Table T_WORKER ( ID INTEGER, DEPT VARCHAR(50), SALARY INTEGER ) )
                Mapping model::WorkerMap ( Worker: Relational { ~mainTable [WorkerDb] T_WORKER dept: [WorkerDb] T_WORKER.DEPT, salary: [WorkerDb] T_WORKER.SALARY } )

                function query::getWorkerInfo(): Any[*]
                {
                    Worker.all()->project([{w | $w.dept}, {w | $w.salary}], ['department', 'sal'])
                }
                """;

        // Setup test data
        connection.createStatement().execute(
                "CREATE TABLE T_WORKER (ID INTEGER, DEPT VARCHAR(50), SALARY INTEGER)");
        connection.createStatement().execute(
                "INSERT INTO T_WORKER VALUES (1, 'Engineering', 100000), (2, 'Engineering', 120000), (3, 'Sales', 80000)");

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(model);
        MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
        pureCompiler = new org.finos.legend.pure.dsl.PureCompiler(mappingRegistry);

        // WHEN: Parse and compile the function BODY (project without nested ->)
        String functionBody = "Worker.all()->project([{w | $w.dept}, {w | $w.salary}], ['department', 'sal'])";
        RelationNode plan = pureCompiler.compile(functionBody);
        String sql = sqlGenerator.generate(plan);

        System.out.println("SQLite Function body SQL (getWorkerInfo): " + sql);

        // Execute against SQLite
        ResultSet rs = connection.createStatement().executeQuery(sql);

        // THEN: Should return 3 rows with projected columns
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            assertNotNull(rs.getString("department"));
            assertTrue(rs.getInt("sal") > 0);
        }
        assertEquals(3, rowCount, "Should have 3 worker rows");
    }
}
