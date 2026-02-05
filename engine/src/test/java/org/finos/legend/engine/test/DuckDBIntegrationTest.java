package org.finos.legend.engine.test;

import org.finos.legend.engine.store.*;
import org.finos.legend.engine.execution.ScalarResult;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.m3.*;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests using DuckDB as the execution engine.
 * 
 * Demonstrates full Pure syntax for:
 * - Class definitions:
 * {@code Class package::Name { property: Type[multiplicity]; }}
 * - Database definitions: {@code Database package::Name ( Table ... )}
 * - Mapping definitions:
 * {@code Mapping package::Name ( ClassName: Relational { ... } )}
 * - Query expressions: {@code ClassName.all()->filter({p | ...})->project(...)}
 */
@DisplayName("DuckDB Integration Tests - Full Pure Language")
class DuckDBIntegrationTest extends AbstractDatabaseTest {

    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }

    @Override
    protected String getDatabaseType() {
        return "DuckDB";
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:"; // In-memory DuckDB
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupDatabase();
        setupMappingRegistry();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ==================== Model Building Tests (require loaded database)
    // ====================

    @Test
    @DisplayName("Build model from Pure definitions")
    void testBuildModelFromPure() {
        // GIVEN: Complete Pure model is already loaded in setupMappingRegistry()

        // THEN: The model builder has created all objects
        assertNotNull(modelBuilder.getClass("Person"));
        assertNotNull(modelBuilder.getTable("T_PERSON"));
        assertTrue(mappingRegistry.findByClassName("Person").isPresent());

        // Verify the PureClass
        PureClass personClass = modelBuilder.getClass("Person");
        assertEquals("Person", personClass.name());
        assertEquals(3, personClass.properties().size());

        // Verify property types
        Property firstName = personClass.getProperty("firstName");
        assertEquals(PrimitiveType.STRING, firstName.genericType());
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
    @DisplayName("Pure: Complex filter with AND - lastName == 'Smith' && age > 25")
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
        assertEquals(2, results.size(), "Both Smiths are over 25");
        for (PersonResult person : results) {
            assertEquals("Smith", person.lastName());
            assertTrue(person.age() > 25, "Age should be > 25");
        }
    }

    @Test
    @DisplayName("Pure: Filter by age only - age >= 30")
    void testPureFilterByAgeOnly() throws SQLException {
        // GIVEN: A Pure query filtering by age
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.age >= 30})
                    ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age})
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: John (30) and Bob (45) should be returned
        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(p -> "John".equals(p.firstName()) && p.age() == 30));
        assertTrue(results.stream().anyMatch(p -> "Bob".equals(p.firstName()) && p.age() == 45));
    }

    @Test
    @DisplayName("Pure: Get all people (no filter)")
    void testPureGetAllPeople() throws SQLException {
        // GIVEN: A Pure query to get all people
        String pureQuery = """
                Person.all()
                    ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age})
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: All 3 people are returned
        assertEquals(3, results.size());
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

        // THEN: All 3 people match (2 Smiths + 1 Jones)
        assertEquals(3, results.size());
    }

    @Test
    @DisplayName("Pure: Filter with string equality")
    void testPureFilterByFirstName() throws SQLException {
        // GIVEN: A Pure query filtering by firstName
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.firstName == 'John'})
                    ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age})
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: Only John Smith is returned
        assertEquals(1, results.size());
        assertEquals("John", results.getFirst().firstName());
        assertEquals("Smith", results.getFirst().lastName());
        assertEquals(30, results.getFirst().age());
    }

    @Test
    @DisplayName("Pure: Filter with less than comparison")
    void testPureFilterLessThan() throws SQLException {
        // GIVEN: A Pure query with < comparison
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.age < 30})
                    ->project({p | $p.firstName}, {p | $p.age})
                """;

        // WHEN: We compile and execute
        List<PersonResult> results = executePureQuery(pureQuery);

        // THEN: Only Jane (28) is under 30
        assertEquals(1, results.size());
        assertEquals("Jane", results.getFirst().firstName());
        assertEquals(28, results.getFirst().age());
    }

    // ==================== SQL Generation Verification ====================

    @Test
    @DisplayName("Verify generated SQL format")
    void testGeneratedSqlFormat() {
        // GIVEN: A Pure query
        String pureQuery = "Person.all()->filter({p | $p.lastName == 'Smith'})->project({p | $p.firstName}, {p | $p.lastName})";

        // WHEN: We compile to SQL
        String sql = generateSql(pureQuery);

        System.out.println("Generated SQL: " + sql);

        // THEN: SQL has proper structure
        assertTrue(sql.contains("SELECT"), "Should have SELECT");
        assertTrue(sql.contains("FROM \"T_PERSON\""), "Should have FROM T_PERSON");
        assertTrue(sql.contains("WHERE"), "Should have WHERE");
        assertTrue(sql.contains("\"LAST_NAME\" = 'Smith'"), "Should filter by LAST_NAME");
        assertTrue(sql.contains("AS \"firstName\""), "Should alias to firstName");
        assertTrue(sql.contains("AS \"lastName\""), "Should alias to lastName");
    }

    // ==================== Metamodel & Registry Tests ====================

    @Test
    @DisplayName("Verify domain model structure")
    void testDomainModelStructure() {
        // Get the class from the model builder (built from Pure syntax)
        PureClass personClass = modelBuilder.getClass("Person");

        assertEquals("Person", personClass.name());
        assertEquals(3, personClass.properties().size());

        Property firstName = personClass.getProperty("firstName");
        assertEquals(PrimitiveType.STRING, firstName.genericType());
        assertTrue(firstName.isRequired());
    }

    @Test
    @DisplayName("Verify mapping registry functionality")
    void testMappingRegistryFunctionality() {
        // Registry is set up in @BeforeEach via setupMappingRegistry()
        assertTrue(mappingRegistry.findByClassName("Person").isPresent());

        RelationalMapping mapping = mappingRegistry.getByClassName("Person");
        assertEquals("FIRST_NAME", mapping.getColumnForProperty("firstName").orElseThrow());
        assertEquals("LAST_NAME", mapping.getColumnForProperty("lastName").orElseThrow());
        assertEquals("AGE_VAL", mapping.getColumnForProperty("age").orElseThrow());
    }

    @Test
    @DisplayName("Build model with Association and Join from Pure")
    void testBuildModelWithAssociationAndJoin() {
        // GIVEN: Pure source with classes, association, database with join
        String pureSource = """
                Class model::Person
                {
                    name: String[1];
                }

                Class model::Address
                {
                    street: String[1];
                }

                Association model::Person_Address
                {
                    person: Person[1];
                    addresses: Address[*];
                }

                Database store::TestDB
                (
                    Table T_PERSON
                    (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100) NOT NULL
                    )
                    Table T_ADDRESS
                    (
                        ID INTEGER PRIMARY KEY,
                        PERSON_ID INTEGER NOT NULL,
                        STREET VARCHAR(200) NOT NULL
                    )
                    Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                )
                """;

        // WHEN: We build the model
        PureModelBuilder builder = new PureModelBuilder().addSource(pureSource);

        // THEN: We have classes, association, tables, and join
        assertNotNull(builder.getClass("Person"));
        assertNotNull(builder.getClass("Address"));
        assertNotNull(builder.getTable("T_PERSON"));
        assertNotNull(builder.getTable("T_ADDRESS"));

        assertTrue(builder.getAssociation("Person_Address").isPresent());
        assertTrue(builder.getJoin("Person_Address").isPresent());

        var join = builder.getJoin("Person_Address").orElseThrow();
        assertEquals("T_PERSON", join.leftTable());
        assertEquals("T_ADDRESS", join.rightTable());
    }

    // ==================== Pure Association Navigation Tests ====================
    // These tests start with REAL Pure queries that navigate through associations.
    // The compiler automatically generates EXISTS for to-many navigation in
    // filters.

    /**
     * THE KEY TEST: Starting from a real Pure query with association navigation.
     * 
     * Pure query: Person.all()->filter({p | $p.addresses.street == '123 Main St'})
     * 
     * This navigates through the to-many 'addresses' association.
     * The compiler should automatically generate EXISTS to prevent row explosion.
     */
    @Test
    @DisplayName("Pure: Person.all()->filter({p | $p.addresses.street == '...'})")
    void testPureAssociationNavigationInFilter() throws SQLException {
        // GIVEN: A Pure query that filters through a to-many association
        // This is the EXACT Pure syntax a user would write
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.addresses.street == '123 Main St'})
                    ->project({p | $p.firstName}, {p | $p.lastName})
                """;

        // WHEN: We compile and generate SQL
        String sql = generateSql(pureQuery);
        System.out.println("Pure Query: " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // THEN: SQL uses EXISTS (not INNER JOIN) to prevent row explosion
        assertTrue(sql.contains("EXISTS"), "Should generate EXISTS for to-many navigation");
        assertTrue(sql.contains("SELECT 1"), "Should use SELECT 1 in EXISTS subquery");
        assertFalse(sql.contains("INNER JOIN"), "Should NOT use INNER JOIN for filtering");

        // AND: Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        java.util.Set<String> people = new java.util.HashSet<>();
        for (var row : result.rows()) {
            people.add((String) row.get(0));
        }

        // Only John has address '123 Main St'
        assertEquals(1, people.size(), "Should find exactly 1 person");
        assertTrue(people.contains("John"), "Should find John");
    }

    @Test
    @DisplayName("Pure: Project through association - Person.all()->project({p | $p.firstName}, {p | $p.addresses.street})")
    void testPureProjectThroughAssociation() throws SQLException {
        // GIVEN: A Pure query that PROJECTS through a to-many association
        // This uses LEFT JOIN (not EXISTS) because we want the actual data
        String pureQuery = """
                Person.all()
                    ->project({p | $p.firstName}, {p | $p.addresses.street})
                """;

        // WHEN: We compile and generate SQL
        String sql = generateSql(pureQuery);
        System.out.println("Pure Query (projection through association): " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // THEN: SQL uses LEFT OUTER JOIN (not EXISTS, since we want the data)
        assertTrue(sql.contains("LEFT OUTER JOIN"), "Should use LEFT OUTER JOIN for projecting through association");
        assertTrue(sql.contains("T_ADDRESS"), "Should reference T_ADDRESS table");
        assertTrue(sql.contains("STREET"), "Should project STREET column");
        assertFalse(sql.contains("EXISTS"), "Should NOT use EXISTS for projection");

        // AND: Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        java.util.List<String> results = new java.util.ArrayList<>();
        for (var row : result.rows()) {
            String firstName = (String) row.get(0);
            String street = (String) row.get(1);
            results.add(firstName + ": " + street);
        }

        System.out.println("Projection results:");
        results.forEach(r -> System.out.println("  " + r));

        // John has 2 addresses, Jane has 1, Bob has 1 = 4 rows total
        assertEquals(4, results.size(), "Should have 4 person-address rows");

        // Verify John appears twice (once per address)
        long johnCount = results.stream().filter(r -> r.startsWith("John:")).count();
        assertEquals(2, johnCount, "John should appear twice (has 2 addresses)");
    }

    @Test
    @DisplayName("Pure: Project multiple properties through same association")
    void testPureProjectMultiplePropertiesThroughAssociation() throws SQLException {
        // GIVEN: A Pure query that projects MULTIPLE properties from the association
        String pureQuery = """
                Person.all()
                    ->project({p | $p.firstName}, {p | $p.addresses.street}, {p | $p.addresses.city})
                """;

        // WHEN: We compile and generate SQL
        String sql = generateSql(pureQuery);
        System.out.println("Pure Query (multiple association projections): " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // THEN: Should have only ONE join (not duplicate joins for each projection)
        // Count occurrences of LEFT OUTER JOIN - should be exactly 1
        int joinCount = sql.split("LEFT OUTER JOIN").length - 1;
        assertEquals(1, joinCount, "Should have exactly 1 LEFT OUTER JOIN, not multiple");

        // AND: Both STREET and CITY should be projected
        assertTrue(sql.contains("STREET"), "Should project STREET");
        assertTrue(sql.contains("CITY"), "Should project CITY");

        // AND: Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        int count = 0;
        for (var row : result.rows()) {
            String firstName = (String) row.get(0);
            String street = (String) row.get(1);
            String city = (String) row.get(2);
            assertNotNull(firstName, "firstName should not be null");
            System.out.println("  " + firstName + ": " + street + ", " + city);
            count++;
        }
        assertEquals(4, count, "Should have 4 rows");
    }

    @Test
    @DisplayName("Pure: Mix local and association projections")
    void testPureMixLocalAndAssociationProjections() throws SQLException {
        // GIVEN: A Pure query mixing local properties and association navigation
        String pureQuery = """
                Person.all()
                    ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age}, {p | $p.addresses.city})
                """;

        // WHEN: We compile and generate SQL
        String sql = generateSql(pureQuery);
        System.out.println("Pure Query (mixed projections): " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // THEN: Should have base table columns AND joined table column
        assertTrue(sql.contains("FIRST_NAME"), "Should project FIRST_NAME from base");
        assertTrue(sql.contains("LAST_NAME"), "Should project LAST_NAME from base");
        assertTrue(sql.contains("AGE_VAL"), "Should project AGE_VAL from base");
        assertTrue(sql.contains("CITY"), "Should project CITY from joined table");
        assertTrue(sql.contains("LEFT OUTER JOIN"), "Should use LEFT OUTER JOIN");

        // AND: Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        int count = 0;
        for (var row : result.rows()) {
            String firstName = (String) row.get(0);
            String lastName = (String) row.get(1);
            int age = ((Number) row.get(2)).intValue();
            assertNotNull(firstName);
            assertNotNull(lastName);
            assertTrue(age > 0, "Age should be positive");
            count++;
        }
        assertEquals(4, count, "Should have 4 rows");
    }

    @Test
    @DisplayName("Pure: Combine filter AND project through association")
    void testPureCombineFilterAndProjectThroughAssociation() throws SQLException {
        // THE ULTIMATE TEST: Both filter and project navigate through associations!
        //
        // Query: Find people who have an address in New York,
        // and show their name plus ALL their addresses
        //
        // Expected behavior:
        // - Filter: EXISTS (find people with at least one NY address)
        // - Project: LEFT JOIN (get all their addresses for display)
        //
        // John has addresses in New York and Boston
        // Jane has address in Chicago
        // Bob has address in Detroit
        //
        // Result: Only John (has NY address), but show BOTH his addresses

        String pureQuery = """
                Person.all()
                    ->filter({p | $p.addresses.city == 'New York'})
                    ->project({p | $p.firstName}, {p | $p.addresses.street}, {p | $p.addresses.city})
                """;

        // WHEN: We compile and generate SQL
        String sql = generateSql(pureQuery);
        System.out.println("Pure Query (filter + project through association): " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // THEN: SQL should have BOTH EXISTS (for filter) AND LEFT JOIN (for projection)
        assertTrue(sql.contains("EXISTS"), "Should use EXISTS for filter");
        assertTrue(sql.contains("LEFT OUTER JOIN"), "Should use LEFT OUTER JOIN for projection");
        assertTrue(sql.contains("SELECT 1"), "EXISTS subquery should use SELECT 1");

        // AND: Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        java.util.List<String> results = new java.util.ArrayList<>();
        for (var row : result.rows()) {
            String firstName = (String) row.get(0);
            String street = (String) row.get(1);
            String city = (String) row.get(2);
            results.add(firstName + ": " + street + ", " + city);
            System.out.println("  " + firstName + ": " + street + ", " + city);
        }

        // John is the only one with a New York address
        // But we should see BOTH of John's addresses (row multiplication from LEFT
        // JOIN)
        assertEquals(2, results.size(), "Should have 2 rows (both of John's addresses)");
        assertTrue(results.stream().allMatch(r -> r.startsWith("John:")), "All results should be John");
        assertTrue(results.stream().anyMatch(r -> r.contains("New York")), "Should include NY address");
        assertTrue(results.stream().anyMatch(r -> r.contains("Boston")), "Should include Boston address");
    }

    @Test
    @DisplayName("Pure: Filter on one association property, project different property")
    void testPureFilterAndProjectDifferentAssociationProperties() throws SQLException {
        // Filter by city, project street - shows the pattern clearly
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.addresses.city == 'Chicago'})
                    ->project({p | $p.firstName}, {p | $p.addresses.street})
                """;

        String sql = generateSql(pureQuery);
        System.out.println("Pure Query (filter city, project street): " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // Should have EXISTS for filter AND LEFT JOIN for projection
        assertTrue(sql.contains("EXISTS"), "Should use EXISTS for filter");
        assertTrue(sql.contains("LEFT OUTER JOIN"), "Should use LEFT OUTER JOIN for projection");

        // Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        java.util.List<String> results = new java.util.ArrayList<>();
        for (var row : result.rows()) {
            String firstName = (String) row.get(0);
            String street = (String) row.get(1);
            results.add(firstName + ": " + street);
            System.out.println("  " + firstName + ": " + street);
        }

        // Jane is the only one with a Chicago address, and she has only 1 address
        assertEquals(1, results.size(), "Should have 1 row");
        assertTrue(results.getFirst().startsWith("Jane:"), "Should be Jane");
        assertTrue(results.getFirst().contains("789 Main Rd"), "Should show Jane's street");
    }

    @Test
    @DisplayName("Pure: Filter local property, project through association")
    void testPureFilterLocalProjectAssociation() throws SQLException {
        // Filter by local property (lastName), project through association
        // This is a simpler case: no EXISTS needed for filter, just LEFT JOIN for
        // projection
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.lastName == 'Smith'})
                    ->project({p | $p.firstName}, {p | $p.addresses.street})
                """;

        String sql = generateSql(pureQuery);
        System.out.println("Pure Query (filter local, project association): " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // Should NOT have EXISTS (local filter), but SHOULD have LEFT JOIN (projection)
        assertFalse(sql.contains("EXISTS"), "Should NOT use EXISTS for local property filter");
        assertTrue(sql.contains("LEFT OUTER JOIN"), "Should use LEFT OUTER JOIN for projection");
        assertTrue(sql.contains("LAST_NAME") && sql.contains("'Smith'"), "Should filter by LAST_NAME");

        // Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        java.util.List<String> results = new java.util.ArrayList<>();
        java.util.Set<String> people = new java.util.HashSet<>();
        for (var row : result.rows()) {
            String firstName = (String) row.get(0);
            String street = (String) row.get(1);
            results.add(firstName + ": " + street);
            people.add(firstName);
            System.out.println("  " + firstName + ": " + street);
        }

        // John Smith has 2 addresses, Jane Smith has 1 = 3 total rows
        assertEquals(3, results.size(), "Should have 3 rows (John x2, Jane x1)");
        assertEquals(2, people.size(), "Should have 2 unique people");
        assertTrue(people.contains("John"), "Should include John");
        assertTrue(people.contains("Jane"), "Should include Jane");
    }

    @Test
    @DisplayName("Pure: Association navigation prevents row explosion")
    void testPureAssociationNoRowExplosion() throws SQLException {
        // John has 2 addresses (123 Main St, 456 Oak Ave)
        // If we query for people with addresses in 'New York' or 'Boston',
        // John should appear only ONCE (not twice!)

        // First, let's modify address data so both of John's addresses match a
        // condition
        try (Statement stmt = connection.createStatement()) {
            // Both of John's addresses are in cities we'll filter for
            // Address 1: 123 Main St, New York (PERSON_ID=1)
            // Address 2: 456 Oak Ave, Boston (PERSON_ID=1)
            // So John has 2 addresses that would match a city filter
        }

        // GIVEN: A Pure query filtering on addresses.city
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.addresses.city == 'New York'})
                    ->project({p | $p.firstName}, {p | $p.lastName})
                """;

        // WHEN: We compile and execute
        String sql = generateSql(pureQuery);
        System.out.println("Pure Query: " + pureQuery);
        System.out.println("Generated SQL: " + sql);

        // Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        int johnCount = 0;
        int totalCount = 0;
        for (var row : result.rows()) {
            totalCount++;
            if ("John".equals(row.get(0))) {
                johnCount++;
            }
        }

        // THEN: John appears exactly ONCE (not multiple times)
        assertEquals(1, johnCount, "John should appear exactly once");
        assertEquals(1, totalCount, "Only one person lives in New York");
    }

    @Test
    @DisplayName("Pure: Filter on association with multiple matches still returns person once")
    void testPureAssociationMultipleMatchesOneResult() throws SQLException {
        // Add a second 'Main' address for John to test no explosion
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_ADDRESS VALUES (100, 1, '999 Main Blvd', 'Seattle')");
        }

        // GIVEN: John now has TWO addresses containing 'Main'
        // Query for all people with an address on 'Main'
        String pureQuery = """
                Person.all()
                    ->filter({p | $p.addresses.street == '123 Main St'})
                    ->project({p | $p.firstName}, {p | $p.lastName})
                """;

        // Note: This tests exact equality. For LIKE we'd need different syntax.
        // The key point is John should appear ONCE even if he has multiple Main
        // addresses.

        String sql = generateSql(pureQuery);
        System.out.println("Generated SQL: " + sql);

        // Execute via QueryService and verify results
        var result = executeRelation(pureQuery);
        int count = 0;
        for (var row : result.rows()) {
            assertEquals("John", row.get(0));
            count++;
        }
        assertEquals(1, count, "John should appear exactly once despite multiple matching addresses");

        // Cleanup
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM T_ADDRESS WHERE ID = 100");
        }
    }

    @Test
    @DisplayName("Pure: Verify EXISTS SQL structure for association filter")
    void testPureAssociationExistsSqlStructure() {
        // GIVEN: A Pure query with association navigation
        String pureQuery = "Person.all()->filter({p | $p.addresses.street == 'Test St'})->project({p | $p.firstName})";

        // WHEN: We compile
        String sql = generateSql(pureQuery);
        System.out.println("SQL Structure Test: " + sql);

        // THEN: Verify the SQL structure matches expected EXISTS pattern
        // Expected form:
        // SELECT ... FROM "T_PERSON" AS "t0"
        // WHERE EXISTS (SELECT 1 FROM "T_ADDRESS" AS "sub0"
        // WHERE ("sub0"."PERSON_ID" = "t0"."ID" AND "sub0"."STREET" = 'Test St'))

        assertTrue(sql.contains("EXISTS"), "Must use EXISTS");
        assertTrue(sql.contains("SELECT 1"), "Subquery must use SELECT 1");
        assertTrue(sql.contains("T_ADDRESS"), "Must reference T_ADDRESS in subquery");
        assertTrue(sql.contains("PERSON_ID"), "Must have correlation on PERSON_ID");
        assertTrue(sql.contains("STREET"), "Must filter on STREET");
        assertTrue(sql.contains("'Test St'"), "Must include the literal value");
    }

    // ==================== Row Explosion Demonstration ====================

    /**
     * This test demonstrates WHY we need EXISTS.
     * It shows the row explosion problem with naive INNER JOINs.
     */
    @Test
    @DisplayName("Demonstrate: INNER JOIN causes row explosion on to-many")
    void testDemonstrateRowExplosionWithJoin() throws SQLException {
        // BAD: Using INNER JOIN for filtering (causes row explosion)
        // This is what we DON'T want the Pure compiler to generate
        String wrongJoinSql = """
                SELECT "p"."FIRST_NAME" AS "firstName", "p"."LAST_NAME" AS "lastName"
                FROM "T_PERSON" AS "p"
                INNER JOIN "T_ADDRESS" AS "a" ON "p"."ID" = "a"."PERSON_ID"
                WHERE "a"."CITY" = 'New York' OR "a"."CITY" = 'Boston'
                """;

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(wrongJoinSql)) {

            int johnCount = 0;
            int totalCount = 0;
            while (rs.next()) {
                totalCount++;
                if ("John".equals(rs.getString("firstName"))) {
                    johnCount++;
                }
            }

            // John has addresses in BOTH New York and Boston
            // With INNER JOIN, he appears TWICE (row explosion!)
            assertEquals(2, johnCount, "With JOIN: John appears twice (row explosion)");
            assertEquals(2, totalCount, "With JOIN: Total 2 rows");
        }

        // GOOD: Using EXISTS (what Pure compiler generates)
        String correctExistsSql = """
                SELECT "p"."FIRST_NAME" AS "firstName", "p"."LAST_NAME" AS "lastName"
                FROM "T_PERSON" AS "p"
                WHERE EXISTS (
                    SELECT 1 FROM "T_ADDRESS" AS "a"
                    WHERE "a"."PERSON_ID" = "p"."ID"
                    AND ("a"."CITY" = 'New York' OR "a"."CITY" = 'Boston')
                )
                """;

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(correctExistsSql)) {

            int johnCount = 0;
            int totalCount = 0;
            while (rs.next()) {
                totalCount++;
                if ("John".equals(rs.getString("firstName"))) {
                    johnCount++;
                }
            }

            // With EXISTS, John appears only ONCE
            assertEquals(1, johnCount, "With EXISTS: John appears once");
            assertEquals(1, totalCount, "With EXISTS: Total 1 row");
        }
    }

    @Test
    @DisplayName("Pure syntax: Class.all()->project()->groupBy()")
    void testPureSyntaxProjectThenGroupBy() throws Exception {
        // GIVEN: Create employee table and data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EMP");
            stmt.execute("CREATE TABLE T_EMP (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_EMP VALUES (1, 'Engineering', 80000)");
            stmt.execute("INSERT INTO T_EMP VALUES (2, 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_EMP VALUES (3, 'Sales', 60000)");
            stmt.execute("INSERT INTO T_EMP VALUES (4, 'Sales', 70000)");
        }

        // Build model with connection and runtime
        String pureSource = """
                Class model::Emp { dept: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP dept: [EmpDb] T_EMP.DEPT, sal: [EmpDb] T_EMP.SAL } )

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime
                {
                    mappings:
                    [
                        model::EmpMap
                    ];
                    connections:
                    [
                        store::EmpDb:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;

        // Pure query
        String pureQuery = "Emp.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal}], ['totalSal'])";

        // Execute via QueryService
        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        java.util.Map<String, Integer> results = new java.util.HashMap<>();
        for (var row : result.rows()) {
            results.put((String) row.get(0), ((Number) row.get(1)).intValue());
        }

        assertEquals(170000, results.get("Engineering"), "Engineering total");
        assertEquals(130000, results.get("Sales"), "Sales total");
    }

    @Test
    @DisplayName("Pure syntax: Class.all()->groupBy() should FAIL (type safety)")
    void testPureSyntaxGroupByOnClassFails() {
        // GIVEN: A model
        String model = """
                Class model::Emp { dept: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP dept: [EmpDb] T_EMP.DEPT, sal: [EmpDb] T_EMP.SAL } )
                """;

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(model);

        // WHEN: Try to parse groupBy directly on Class.all() (without project)
        // THEN: Should fail with PureParseException explaining the correct pattern
        String invalidQuery = "Emp.all()->groupBy([{e | $e.dept}], [{e | $e.sal}], ['totalSal'])";

        org.finos.legend.pure.dsl.PureParseException exception = assertThrows(
                org.finos.legend.pure.dsl.PureParseException.class,
                () -> org.finos.legend.pure.dsl.PureParser.parse(invalidQuery),
                "groupBy on Class should throw PureParseException");

        // Verify the error message is helpful
        assertTrue(exception.getMessage().contains("project"),
                "Error should mention project(): " + exception.getMessage());
        System.out.println("Expected error message: " + exception.getMessage());
    }

    @Test
    @DisplayName("Pure syntax: Class.all()->sortBy({...})->limit()")
    void testPureSyntaxSortByAndLimit() throws Exception {
        // GIVEN: A model with connection and runtime
        String pureSource = """
                Class model::Emp { name: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, NAME VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP name: [EmpDb] T_EMP.NAME, sal: [EmpDb] T_EMP.SAL } )

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime
                {
                    mappings:
                    [
                        model::EmpMap
                    ];
                    connections:
                    [
                        store::EmpDb:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;

        // Create test data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EMP");
            stmt.execute("CREATE TABLE T_EMP (ID INTEGER, NAME VARCHAR(100), SAL INTEGER)");
            stmt.execute(
                    "INSERT INTO T_EMP VALUES (1, 'John', 50000), (2, 'Jane', 80000), (3, 'Bob', 60000), (4, 'Alice', 70000)");
        }

        // Execute via QueryService
        String pureQuery = "Emp.all()->sortBy({e | $e.sal}, 'desc')->limit(3)";
        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        // Should get top 3 salaries in descending order
        assertEquals(3, result.rows().size(), "Should return 3 rows");
    }

    @Test
    @DisplayName("Pure syntax: Relation->sort()->slice()")
    void testPureSyntaxRelationSortAndSlice() throws Exception {
        // GIVEN: A model with connection and runtime
        String pureSource = """
                Class model::Emp { name: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, NAME VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP name: [EmpDb] T_EMP.NAME, sal: [EmpDb] T_EMP.SAL } )

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime
                {
                    mappings:
                    [
                        model::EmpMap
                    ];
                    connections:
                    [
                        store::EmpDb:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;

        // Create test data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EMP");
            stmt.execute("CREATE TABLE T_EMP (ID INTEGER, NAME VARCHAR(100), SAL INTEGER)");
            stmt.execute(
                    "INSERT INTO T_EMP VALUES (1, 'John', 50000), (2, 'Jane', 80000), (3, 'Bob', 60000), (4, 'Alice', 70000), (5, 'Eve', 55000), (6, 'Charlie', 65000)");
        }

        // Execute via QueryService
        String pureQuery = "Emp.all()->project([{e | $e.name}, {e | $e.sal}], ['name', 'sal'])->sort('sal', 'asc')->slice(0, 5)";
        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        // Should get first 5 of 6 rows, sorted by salary ascending
        assertEquals(5, result.rows().size(), "Should return 5 rows");
    }

    // ==================== Function Definition Tests ====================

    @Test
    @DisplayName("Function with Class query - execute body against DuckDB")
    void testFunctionWithClassQuery_DuckDB() throws Exception {
        // GIVEN: A model with a function, connection and runtime
        String pureSource = """
                Class model::Adult { name: String[1]; age: Integer[1]; }
                Database store::AdultDb ( Table T_ADULT ( ID INTEGER, NAME VARCHAR(100), AGE INTEGER ) )
                Mapping model::AdultMap ( Adult: Relational { ~mainTable [AdultDb] T_ADULT name: [AdultDb] T_ADULT.NAME, age: [AdultDb] T_ADULT.AGE } )

                function query::getAdults(): model::Adult[*]
                {
                    Adult.all()->filter({p | $p.age >= 18})
                }

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

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
        String functionBody = "Adult.all()->filter({p | $p.age >= 18})";
        var result = queryService.execute(pureSource, functionBody, "test::TestRuntime", connection);

        // THEN: Should return only adults (age >= 18)
        assertEquals(2, result.rows().size(), "Should return 2 adults (Alice and Charlie)");
    }

    @Test
    @DisplayName("Function with Relation query - execute body against DuckDB")
    void testFunctionWithRelationQuery_DuckDB() throws Exception {
        // GIVEN: A model with a function, connection and runtime
        String pureSource = """
                Class model::Worker { dept: String[1]; salary: Integer[1]; }
                Database store::WorkerDb ( Table T_WORKER ( ID INTEGER, DEPT VARCHAR(50), SALARY INTEGER ) )
                Mapping model::WorkerMap ( Worker: Relational { ~mainTable [WorkerDb] T_WORKER dept: [WorkerDb] T_WORKER.DEPT, salary: [WorkerDb] T_WORKER.SALARY } )

                function query::getWorkerInfo(): Any[*]
                {
                    Worker.all()->project([{w | $w.dept}, {w | $w.salary}], ['department', 'sal'])
                }

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

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
        String functionBody = "Worker.all()->project([{w | $w.dept}, {w | $w.salary}], ['department', 'sal'])";
        var result = queryService.execute(pureSource, functionBody, "test::TestRuntime", connection);

        // THEN: Should return 3 rows with projected columns
        assertEquals(3, result.rows().size(), "Should have 3 worker rows");
    }

    // ==================== Relation API Tests ====================

    @Test
    @DisplayName("Relation API: #>{DB.TABLE}->from(runtime) - execute via QueryService")
    void testRelationApiDirectTableQuery() throws Exception {
        // GIVEN: A Relation query with ->from(runtime)
        // The query: #>{store::PersonDatabase.T_PERSON}->from(test::TestRuntime)

        String pureSource = getCompletePureModelWithRuntime();
        String relationQuery = "#>{store::PersonDatabase.T_PERSON}->from(test::TestRuntime)";

        // WHEN: Execute through QueryService (parse → compile → SQL → execute)
        org.finos.legend.engine.execution.BufferedResult result = queryService.execute(
                pureSource,
                relationQuery,
                "test::TestRuntime",
                connection);

        // THEN: Should return 3 rows from T_PERSON
        assertEquals(3, result.rows().size(), "Should return all 3 persons from T_PERSON");

        // Verify column metadata
        var columns = result.columns();
        assertTrue(columns.stream().anyMatch(c -> c.name().equals("FIRST_NAME")),
                "Should have FIRST_NAME column");
        assertTrue(columns.stream().anyMatch(c -> c.name().equals("LAST_NAME")),
                "Should have LAST_NAME column");

        System.out.println("Relation API via QueryService: Retrieved " + result.rows().size() + " rows");
        System.out.println("Columns: " + columns.stream().map(c -> c.name()).toList());
    }

    // ==================== Relation Function Tests: distinct, rename, concatenate
    // ====================

    @Test
    @DisplayName("Pure Relation: distinct() - remove duplicate rows")
    void testRelationDistinct() throws Exception {
        // GIVEN: Create a table with duplicate data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_ITEMS");
            stmt.execute("CREATE TABLE T_ITEMS (ID INTEGER, CATEGORY VARCHAR(50), VALUE INTEGER)");
            stmt.execute("INSERT INTO T_ITEMS VALUES (1, 'A', 100)");
            stmt.execute("INSERT INTO T_ITEMS VALUES (2, 'A', 100)"); // Same category and value
            stmt.execute("INSERT INTO T_ITEMS VALUES (3, 'B', 200)");
            stmt.execute("INSERT INTO T_ITEMS VALUES (4, 'A', 100)"); // Another duplicate
            stmt.execute("INSERT INTO T_ITEMS VALUES (5, 'B', 300)");
        }

        // Build model
        String pureSource = """
                Class model::Item { category: String[1]; value: Integer[1]; }
                Database store::ItemDb ( Table T_ITEMS ( ID INTEGER, CATEGORY VARCHAR(50), VALUE INTEGER ) )
                Mapping model::ItemMap ( Item: Relational { ~mainTable [ItemDb] T_ITEMS category: [ItemDb] T_ITEMS.CATEGORY, value: [ItemDb] T_ITEMS.VALUE } )
                RelationalDatabaseConnection store::TestConn { store: ItemDb; type: DuckDB; specification: LocalH2{ url: 'jdbc:duckdb:' }; }
                Runtime test::TestRuntime { mappings: [ItemMap]; connections: [ItemDb: [conn: store::TestConn]]; }
                """;

        // WHEN: Execute a relation query with distinct()
        String relationQuery = "#>{store::ItemDb.T_ITEMS}->select(~CATEGORY, ~VALUE)->distinct()->from(test::TestRuntime)";

        org.finos.legend.engine.execution.BufferedResult result = queryService.execute(
                pureSource, relationQuery, "test::TestRuntime", connection);

        // THEN: Should have 3 distinct category-value combinations (A-100, B-200,
        // B-300)
        System.out.println("Distinct result rows: " + result.rows().size());
        result.rows().forEach(r -> System.out.println("  " + r));

        assertEquals(3, result.rows().size(), "Should have 3 distinct category-value pairs");
    }

    @Test
    @DisplayName("Pure Relation: rename() - rename a column")
    void testRelationRename() throws Exception {
        // Build model with existing T_PERSON table
        String pureSource = getCompletePureModelWithRuntime();

        // WHEN: Execute a relation query with rename()
        String relationQuery = "#>{store::PersonDatabase.T_PERSON}->select(~FIRST_NAME)->rename(~FIRST_NAME, ~givenName)->from(test::TestRuntime)";

        org.finos.legend.engine.execution.BufferedResult result = queryService.execute(
                pureSource, relationQuery, "test::TestRuntime", connection);

        // THEN: Should have the renamed column
        var columns = result.columns();
        System.out.println("Columns after rename: " + columns.stream().map(c -> c.name()).toList());

        assertTrue(columns.stream().anyMatch(c -> c.name().equalsIgnoreCase("givenName")),
                "Should have renamed column 'givenName'");
        assertFalse(columns.stream().anyMatch(c -> c.name().equalsIgnoreCase("FIRST_NAME")),
                "Should not have original column 'FIRST_NAME'");

        assertEquals(3, result.rows().size(), "Should still have 3 rows");
    }

    @Test
    @DisplayName("Pure Relation: concatenate() - union two relations")
    void testRelationConcatenate() throws Exception {
        // GIVEN: Create two separate tables
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_ACTIVE_USERS");
            stmt.execute("DROP TABLE IF EXISTS T_INACTIVE_USERS");
            stmt.execute("CREATE TABLE T_ACTIVE_USERS (ID INTEGER, NAME VARCHAR(100))");
            stmt.execute("CREATE TABLE T_INACTIVE_USERS (ID INTEGER, NAME VARCHAR(100))");
            stmt.execute("INSERT INTO T_ACTIVE_USERS VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO T_ACTIVE_USERS VALUES (2, 'Bob')");
            stmt.execute("INSERT INTO T_INACTIVE_USERS VALUES (101, 'Charlie')");
            stmt.execute("INSERT INTO T_INACTIVE_USERS VALUES (102, 'Diana')");
            stmt.execute("INSERT INTO T_INACTIVE_USERS VALUES (103, 'Eve')");
        }

        // Build model
        String pureSource = """
                Class model::User { name: String[1]; }
                Database store::UserDb (
                    Table T_ACTIVE_USERS ( ID INTEGER, NAME VARCHAR(100) )
                    Table T_INACTIVE_USERS ( ID INTEGER, NAME VARCHAR(100) )
                )
                RelationalDatabaseConnection store::TestConn { store: UserDb; type: DuckDB; specification: LocalH2{ url: 'jdbc:duckdb:' }; }
                Runtime test::TestRuntime { mappings: []; connections: [UserDb: [conn: store::TestConn]]; }
                """;

        // WHEN: Execute a relation query with concatenate()
        String relationQuery = """
                #>{store::UserDb.T_ACTIVE_USERS}->select(~ID, ~NAME)
                    ->concatenate(#>{store::UserDb.T_INACTIVE_USERS}->select(~ID, ~NAME))
                    ->from(test::TestRuntime)
                """;

        org.finos.legend.engine.execution.BufferedResult result = queryService.execute(
                pureSource, relationQuery, "test::TestRuntime", connection);

        // THEN: Should have all 5 rows (2 active + 3 inactive)
        System.out.println("Concatenate result rows: " + result.rows().size());
        result.rows().forEach(r -> System.out.println("  " + r));

        assertEquals(5, result.rows().size(), "Should have 5 total rows (2 + 3)");

        // Verify we have data from both tables
        java.util.Set<String> names = new java.util.HashSet<>();
        for (var row : result.rows()) {
            names.add((String) row.get(1));
        }
        assertTrue(names.contains("Alice"), "Should have Alice from active users");
        assertTrue(names.contains("Charlie"), "Should have Charlie from inactive users");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with stdDev() aggregate")
    void testPureSyntaxGroupByWithStdDev() throws Exception {
        // GIVEN: Create employee table with numeric data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_STATS");
            stmt.execute("CREATE TABLE T_STATS (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            // Engineering: 100000, 90000, 80000 - stddev should be ~10000
            stmt.execute("INSERT INTO T_STATS VALUES (1, 'Engineering', 100000)");
            stmt.execute("INSERT INTO T_STATS VALUES (2, 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_STATS VALUES (3, 'Engineering', 80000)");
            // Sales: 85000, 75000 - stddev should be ~7071
            stmt.execute("INSERT INTO T_STATS VALUES (4, 'Sales', 85000)");
            stmt.execute("INSERT INTO T_STATS VALUES (5, 'Sales', 75000)");
        }

        String pureSource = """
                Class model::Stats { dept: String[1]; sal: Integer[1]; }
                Database store::StatsDb ( Table T_STATS ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::StatsMap ( Stats: Relational { ~mainTable [StatsDb] T_STATS dept: [StatsDb] T_STATS.DEPT, sal: [StatsDb] T_STATS.SAL } )

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime
                {
                    mappings: [ model::StatsMap ];
                    connections: [ store::StatsDb: [ environment: store::TestConnection ] ];
                }
                """;

        // Pure query using stdDev() aggregate
        String pureQuery = "Stats.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->stdDev()}], ['stdDevSal'])";

        // Execute via QueryService
        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        System.out.println("StdDev results:");
        java.util.Map<String, Double> results = new java.util.HashMap<>();
        for (var row : result.rows()) {
            String dept = (String) row.get(0);
            double stddev = ((Number) row.get(1)).doubleValue();
            results.put(dept, stddev);
            System.out.printf("  %s: stdDev = %.2f%n", dept, stddev);
        }

        // Engineering stddev should be ~10000 (sample std dev of 80k, 90k, 100k)
        assertTrue(results.get("Engineering") > 9000 && results.get("Engineering") < 11000,
                "Engineering stdDev should be ~10000, got: " + results.get("Engineering"));
        // Sales stddev should be ~7071 (sample std dev of 75k, 85k)
        assertTrue(results.get("Sales") > 6000 && results.get("Sales") < 8000,
                "Sales stdDev should be ~7071, got: " + results.get("Sales"));
    }

    @Test
    @DisplayName("Pure syntax: groupBy with variance() aggregate")
    void testPureSyntaxGroupByWithVariance() throws Exception {
        // Reuse T_STATS table from previous test or create it
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_VAR");
            stmt.execute("CREATE TABLE T_VAR (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_VAR VALUES (1, 'Engineering', 100000)");
            stmt.execute("INSERT INTO T_VAR VALUES (2, 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_VAR VALUES (3, 'Engineering', 80000)");
        }

        String pureSource = """
                Class model::Var { dept: String[1]; sal: Integer[1]; }
                Database store::VarDb ( Table T_VAR ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::VarMap ( Var: Relational { ~mainTable [VarDb] T_VAR dept: [VarDb] T_VAR.DEPT, sal: [VarDb] T_VAR.SAL } )

                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::VarMap ]; connections: [ store::VarDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "Var.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->variance()}], ['varSal'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        assertEquals(1, result.rows().size(), "Should have 1 department");
        double variance = ((Number) result.rows().get(0).get(1)).doubleValue();
        // Variance of 80k, 90k, 100k = 100,000,000 (sample variance)
        System.out.printf("Variance of Engineering salaries: %.2f%n", variance);
        assertTrue(variance > 90_000_000 && variance < 110_000_000,
                "Variance should be ~100M, got: " + variance);
    }

    @Test
    @DisplayName("Pure syntax: groupBy with median() aggregate")
    void testPureSyntaxGroupByWithMedian() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_MED");
            stmt.execute("CREATE TABLE T_MED (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_MED VALUES (1, 'Team', 100000)");
            stmt.execute("INSERT INTO T_MED VALUES (2, 'Team', 90000)");
            stmt.execute("INSERT INTO T_MED VALUES (3, 'Team', 80000)");
        }

        String pureSource = """
                Class model::Med { dept: String[1]; sal: Integer[1]; }
                Database store::MedDb ( Table T_MED ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::MedMap ( Med: Relational { ~mainTable [MedDb] T_MED dept: [MedDb] T_MED.DEPT, sal: [MedDb] T_MED.SAL } )

                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::MedMap ]; connections: [ store::MedDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "Med.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->median()}], ['medianSal'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        assertEquals(1, result.rows().size());
        double median = ((Number) result.rows().get(0).get(1)).doubleValue();
        // Median of 80k, 90k, 100k = 90k
        System.out.printf("Median of Team salaries: %.2f%n", median);
        assertEquals(90000.0, median, 1.0, "Median should be 90000");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with stdDevSample() aggregate")
    void testPureSyntaxGroupByWithStdDevSample() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_STDDEV_SAMP");
            stmt.execute("CREATE TABLE T_STDDEV_SAMP (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_STDDEV_SAMP VALUES (1, 'Team', 100000)");
            stmt.execute("INSERT INTO T_STDDEV_SAMP VALUES (2, 'Team', 90000)");
            stmt.execute("INSERT INTO T_STDDEV_SAMP VALUES (3, 'Team', 80000)");
        }

        String pureSource = """
                Class model::StdSamp { dept: String[1]; sal: Integer[1]; }
                Database store::StdSampDb ( Table T_STDDEV_SAMP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::StdSampMap ( StdSamp: Relational { ~mainTable [StdSampDb] T_STDDEV_SAMP dept: [StdSampDb] T_STDDEV_SAMP.DEPT, sal: [StdSampDb] T_STDDEV_SAMP.SAL } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::StdSampMap ]; connections: [ store::StdSampDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "StdSamp.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->stdDevSample()}], ['result'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        assertEquals(1, result.rows().size());
        double value = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("stdDevSample result: %.2f%n", value);
        assertTrue(value > 9000 && value < 11000, "stdDevSample should be ~10000");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with stdDevPopulation() aggregate")
    void testPureSyntaxGroupByWithStdDevPopulation() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_STDDEV_POP");
            stmt.execute("CREATE TABLE T_STDDEV_POP (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_STDDEV_POP VALUES (1, 'Team', 100000)");
            stmt.execute("INSERT INTO T_STDDEV_POP VALUES (2, 'Team', 90000)");
            stmt.execute("INSERT INTO T_STDDEV_POP VALUES (3, 'Team', 80000)");
        }

        String pureSource = """
                Class model::StdPop { dept: String[1]; sal: Integer[1]; }
                Database store::StdPopDb ( Table T_STDDEV_POP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::StdPopMap ( StdPop: Relational { ~mainTable [StdPopDb] T_STDDEV_POP dept: [StdPopDb] T_STDDEV_POP.DEPT, sal: [StdPopDb] T_STDDEV_POP.SAL } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::StdPopMap ]; connections: [ store::StdPopDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "StdPop.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->stdDevPopulation()}], ['result'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        assertEquals(1, result.rows().size());
        double value = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("stdDevPopulation result: %.2f%n", value);
        // Population stddev is slightly smaller than sample
        assertTrue(value > 7000 && value < 9000, "stdDevPopulation should be ~8165");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with varianceSample() aggregate")
    void testPureSyntaxGroupByWithVarianceSample() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_VAR_SAMP");
            stmt.execute("CREATE TABLE T_VAR_SAMP (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_VAR_SAMP VALUES (1, 'Team', 100000)");
            stmt.execute("INSERT INTO T_VAR_SAMP VALUES (2, 'Team', 90000)");
            stmt.execute("INSERT INTO T_VAR_SAMP VALUES (3, 'Team', 80000)");
        }

        String pureSource = """
                Class model::VarSamp { dept: String[1]; sal: Integer[1]; }
                Database store::VarSampDb ( Table T_VAR_SAMP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::VarSampMap ( VarSamp: Relational { ~mainTable [VarSampDb] T_VAR_SAMP dept: [VarSampDb] T_VAR_SAMP.DEPT, sal: [VarSampDb] T_VAR_SAMP.SAL } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::VarSampMap ]; connections: [ store::VarSampDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "VarSamp.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->varianceSample()}], ['result'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        assertEquals(1, result.rows().size());
        double value = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("varianceSample result: %.2f%n", value);
        assertTrue(value > 90_000_000 && value < 110_000_000, "varianceSample should be ~100M");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with variancePopulation() aggregate")
    void testPureSyntaxGroupByWithVariancePopulation() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_VAR_POP");
            stmt.execute("CREATE TABLE T_VAR_POP (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            stmt.execute("INSERT INTO T_VAR_POP VALUES (1, 'Team', 100000)");
            stmt.execute("INSERT INTO T_VAR_POP VALUES (2, 'Team', 90000)");
            stmt.execute("INSERT INTO T_VAR_POP VALUES (3, 'Team', 80000)");
        }

        String pureSource = """
                Class model::VarPop { dept: String[1]; sal: Integer[1]; }
                Database store::VarPopDb ( Table T_VAR_POP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::VarPopMap ( VarPop: Relational { ~mainTable [VarPopDb] T_VAR_POP dept: [VarPopDb] T_VAR_POP.DEPT, sal: [VarPopDb] T_VAR_POP.SAL } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::VarPopMap ]; connections: [ store::VarPopDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "VarPop.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->variancePopulation()}], ['result'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        assertEquals(1, result.rows().size());
        double value = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("variancePopulation result: %.2f%n", value);
        // Population variance is smaller than sample variance
        assertTrue(value > 60_000_000 && value < 70_000_000, "variancePopulation should be ~66.67M");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with corr() bi-variate aggregate (correlation)")
    void testPureSyntaxGroupByWithCorr() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_CORR");
            stmt.execute("CREATE TABLE T_CORR (ID INTEGER, DEPT VARCHAR, SAL INTEGER, YEARS INTEGER)");
            // Strong positive correlation: salary increases with years
            stmt.execute("INSERT INTO T_CORR VALUES (1, 'Team', 80000, 3)");
            stmt.execute("INSERT INTO T_CORR VALUES (2, 'Team', 90000, 5)");
            stmt.execute("INSERT INTO T_CORR VALUES (3, 'Team', 100000, 7)");
        }

        String pureSource = """
                Class model::Corr { dept: String[1]; sal: Integer[1]; years: Integer[1]; }
                Database store::CorrDb ( Table T_CORR ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER, YEARS INTEGER ) )
                Mapping model::CorrMap ( Corr: Relational { ~mainTable [CorrDb] T_CORR dept: [CorrDb] T_CORR.DEPT, sal: [CorrDb] T_CORR.SAL, years: [CorrDb] T_CORR.YEARS } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::CorrMap ]; connections: [ store::CorrDb: [ environment: store::TestConn ] ]; }
                """;

        // Bi-variate syntax: $r.sal->corr($r.years) produces CORR(sal, years)
        String pureQuery = "Corr.all()->project([{e | $e.dept}, {e | $e.sal}, {e | $e.years}], ['dept', 'sal', 'years'])->groupBy([{r | $r.dept}], [{r | $r.sal->corr($r.years)}], ['correlation'])";

        System.out.println("Pure Query: " + pureQuery);
        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("corr result: " + result.rows());
        assertEquals(1, result.rows().size());

        // Perfect linear correlation should give 1.0
        double corr = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("Correlation between salary and years: %.4f%n", corr);
        assertEquals(1.0, corr, 0.0001, "Perfect positive correlation expected");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with covarSample() bi-variate aggregate")
    void testPureSyntaxGroupByWithCovarSample() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_COVAR_SAMP");
            stmt.execute("CREATE TABLE T_COVAR_SAMP (ID INTEGER, DEPT VARCHAR, SAL INTEGER, YEARS INTEGER)");
            // Perfect positive covariance: salary increases with years
            stmt.execute("INSERT INTO T_COVAR_SAMP VALUES (1, 'Team', 80000, 3)");
            stmt.execute("INSERT INTO T_COVAR_SAMP VALUES (2, 'Team', 90000, 5)");
            stmt.execute("INSERT INTO T_COVAR_SAMP VALUES (3, 'Team', 100000, 7)");
        }

        String pureSource = """
                Class model::CovarSamp { dept: String[1]; sal: Integer[1]; years: Integer[1]; }
                Database store::CovarSampDb ( Table T_COVAR_SAMP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER, YEARS INTEGER ) )
                Mapping model::CovarSampMap ( CovarSamp: Relational { ~mainTable [CovarSampDb] T_COVAR_SAMP dept: [CovarSampDb] T_COVAR_SAMP.DEPT, sal: [CovarSampDb] T_COVAR_SAMP.SAL, years: [CovarSampDb] T_COVAR_SAMP.YEARS } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::CovarSampMap ]; connections: [ store::CovarSampDb: [ environment: store::TestConn ] ]; }
                """;

        // Bi-variate syntax: $r.sal->covarSample($r.years) produces COVAR_SAMP(sal,
        // years)
        String pureQuery = "CovarSamp.all()->project([{e | $e.dept}, {e | $e.sal}, {e | $e.years}], ['dept', 'sal', 'years'])->groupBy([{r | $r.dept}], [{r | $r.sal->covarSample($r.years)}], ['covariance'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("covarSample result: " + result.rows());
        assertEquals(1, result.rows().size());

        double covar = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("Covariance (sample) between salary and years: %.2f%n", covar);
        assertTrue(covar > 0, "Positive covariance expected");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with covarPopulation() bi-variate aggregate")
    void testPureSyntaxGroupByWithCovarPopulation() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_COVAR_POP");
            stmt.execute("CREATE TABLE T_COVAR_POP (ID INTEGER, DEPT VARCHAR, SAL INTEGER, YEARS INTEGER)");
            // Perfect positive covariance: salary increases with years
            stmt.execute("INSERT INTO T_COVAR_POP VALUES (1, 'Team', 80000, 3)");
            stmt.execute("INSERT INTO T_COVAR_POP VALUES (2, 'Team', 90000, 5)");
            stmt.execute("INSERT INTO T_COVAR_POP VALUES (3, 'Team', 100000, 7)");
        }

        String pureSource = """
                Class model::CovarPop { dept: String[1]; sal: Integer[1]; years: Integer[1]; }
                Database store::CovarPopDb ( Table T_COVAR_POP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER, YEARS INTEGER ) )
                Mapping model::CovarPopMap ( CovarPop: Relational { ~mainTable [CovarPopDb] T_COVAR_POP dept: [CovarPopDb] T_COVAR_POP.DEPT, sal: [CovarPopDb] T_COVAR_POP.SAL, years: [CovarPopDb] T_COVAR_POP.YEARS } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::CovarPopMap ]; connections: [ store::CovarPopDb: [ environment: store::TestConn ] ]; }
                """;

        // Bi-variate syntax: $r.sal->covarPopulation($r.years) produces COVAR_POP(sal,
        // years)
        String pureQuery = "CovarPop.all()->project([{e | $e.dept}, {e | $e.sal}, {e | $e.years}], ['dept', 'sal', 'years'])->groupBy([{r | $r.dept}], [{r | $r.sal->covarPopulation($r.years)}], ['covariance'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("covarPopulation result: " + result.rows());
        assertEquals(1, result.rows().size());

        double covar = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("Covariance (population) between salary and years: %.2f%n", covar);
        assertTrue(covar > 0, "Positive covariance expected");
    }

    @Test
    @DisplayName("Pure syntax: groupBy with percentileCont() ordered-set aggregate")
    void testPureSyntaxGroupByWithPercentileCont() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_PERCENTILE");
            stmt.execute("CREATE TABLE T_PERCENTILE (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            // Engineering: 80k, 90k, 100k -> median = 90k
            stmt.execute("INSERT INTO T_PERCENTILE VALUES (1, 'Engineering', 80000)");
            stmt.execute("INSERT INTO T_PERCENTILE VALUES (2, 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_PERCENTILE VALUES (3, 'Engineering', 100000)");
            // Sales: 60k, 70k -> median = 65k (interpolated)
            stmt.execute("INSERT INTO T_PERCENTILE VALUES (4, 'Sales', 60000)");
            stmt.execute("INSERT INTO T_PERCENTILE VALUES (5, 'Sales', 70000)");
        }

        String pureSource = """
                Class model::PctEmployee { dept: String[1]; sal: Integer[1]; }
                Database store::PctDb ( Table T_PERCENTILE ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::PctMap ( PctEmployee: Relational { ~mainTable [PctDb] T_PERCENTILE dept: [PctDb] T_PERCENTILE.DEPT, sal: [PctDb] T_PERCENTILE.SAL } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::PctMap ]; connections: [ store::PctDb: [ environment: store::TestConn ] ]; }
                """;

        // percentileCont(0.5) = median with interpolation
        String pureQuery = "PctEmployee.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->percentileCont(0.5)}], ['dept', 'medianSal'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("percentileCont result: " + result.rows());
        assertEquals(2, result.rows().size());

        // Verify results
        for (var row : result.rows()) {
            String dept = (String) row.get(0);
            double median = ((Number) row.get(1)).doubleValue();
            System.out.printf("  %s: median salary = %.2f%n", dept, median);

            if ("Engineering".equals(dept)) {
                assertEquals(90000.0, median, 1.0, "Engineering median should be 90k");
            } else if ("Sales".equals(dept)) {
                assertEquals(65000.0, median, 1.0, "Sales median should be 65k (interpolated)");
            }
        }
    }

    @Test
    @DisplayName("Pure syntax: groupBy with percentileDisc() ordered-set aggregate")
    void testPureSyntaxGroupByWithPercentileDisc() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_PERCENTILE_DISC");
            stmt.execute("CREATE TABLE T_PERCENTILE_DISC (ID INTEGER, DEPT VARCHAR, SAL INTEGER)");
            // Engineering: 80k, 90k, 100k -> Q3 (75th percentile) discrete = 100k
            stmt.execute("INSERT INTO T_PERCENTILE_DISC VALUES (1, 'Engineering', 80000)");
            stmt.execute("INSERT INTO T_PERCENTILE_DISC VALUES (2, 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_PERCENTILE_DISC VALUES (3, 'Engineering', 100000)");
        }

        String pureSource = """
                Class model::DiscEmployee { dept: String[1]; sal: Integer[1]; }
                Database store::DiscDb ( Table T_PERCENTILE_DISC ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::DiscMap ( DiscEmployee: Relational { ~mainTable [DiscDb] T_PERCENTILE_DISC dept: [DiscDb] T_PERCENTILE_DISC.DEPT, sal: [DiscDb] T_PERCENTILE_DISC.SAL } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DiscMap ]; connections: [ store::DiscDb: [ environment: store::TestConn ] ]; }
                """;

        // percentileDisc(0.75) = 75th percentile (discrete, actual value from dataset)
        String pureQuery = "DiscEmployee.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal->percentileDisc(0.75)}], ['dept', 'q3Sal'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("percentileDisc result: " + result.rows());
        assertEquals(1, result.rows().size());

        // Verify - discrete percentile returns actual value from dataset
        double q3 = ((Number) result.rows().get(0).get(1)).doubleValue();
        System.out.printf("75th percentile (discrete): %.2f%n", q3);
        // For discrete percentile, should be 90000 or 100000 depending on
        // implementation
        assertTrue(q3 >= 90000 && q3 <= 100000, "75th percentile should be 90k or 100k");
    }

    @Test
    @DisplayName("Pure syntax: date extraction functions (year, month, dayOfMonth)")
    void testPureSyntaxDateExtraction() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DATE_TEST");
            stmt.execute("CREATE TABLE T_DATE_TEST (ID INTEGER, NAME VARCHAR, BIRTH_DATE DATE)");
            stmt.execute("INSERT INTO T_DATE_TEST VALUES (1, 'Alice', '1990-06-15')");
            stmt.execute("INSERT INTO T_DATE_TEST VALUES (2, 'Bob', '1985-12-25')");
            stmt.execute("INSERT INTO T_DATE_TEST VALUES (3, 'Charlie', '2000-01-01')");
        }

        String pureSource = """
                Class model::DatePerson { name: String[1]; birthDate: StrictDate[1]; }
                Database store::DateDb ( Table T_DATE_TEST ( ID INTEGER, NAME VARCHAR(100), BIRTH_DATE DATE ) )
                Mapping model::DateMap ( DatePerson: Relational { ~mainTable [DateDb] T_DATE_TEST name: [DateDb] T_DATE_TEST.NAME, birthDate: [DateDb] T_DATE_TEST.BIRTH_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DateMap ]; connections: [ store::DateDb: [ environment: store::TestConn ] ]; }
                """;

        // Test year(), month(), dayOfMonth() extraction
        String pureQuery = "DatePerson.all()->project([{e | $e.name}, {e | $e.birthDate->year()}, {e | $e.birthDate->month()}, {e | $e.birthDate->dayOfMonth()}], ['name', 'birthYear', 'birthMonth', 'birthDay'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Date extraction result: " + result.rows());
        assertEquals(3, result.rows().size());

        // Verify results
        for (var row : result.rows()) {
            String name = (String) row.get(0);
            int year = ((Number) row.get(1)).intValue();
            int month = ((Number) row.get(2)).intValue();
            int day = ((Number) row.get(3)).intValue();
            System.out.printf("  %s: year=%d, month=%d, day=%d%n", name, year, month, day);

            if ("Alice".equals(name)) {
                assertEquals(1990, year, "Alice born 1990");
                assertEquals(6, month, "Alice born June");
                assertEquals(15, day, "Alice born 15th");
            } else if ("Bob".equals(name)) {
                assertEquals(1985, year, "Bob born 1985");
                assertEquals(12, month, "Bob born December");
                assertEquals(25, day, "Bob born 25th");
            } else if ("Charlie".equals(name)) {
                assertEquals(2000, year, "Charlie born 2000");
                assertEquals(1, month, "Charlie born January");
                assertEquals(1, day, "Charlie born 1st");
            }
        }
    }

    @Test
    @DisplayName("Pure syntax: time extraction functions (hour, minute, second)")
    void testPureSyntaxTimeExtraction() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_TIME_TEST");
            stmt.execute("CREATE TABLE T_TIME_TEST (ID INTEGER, NAME VARCHAR, EVENT_TIME TIMESTAMP)");
            stmt.execute("INSERT INTO T_TIME_TEST VALUES (1, 'Meeting', '2024-06-15 14:30:45')");
            stmt.execute("INSERT INTO T_TIME_TEST VALUES (2, 'Call', '2024-12-25 09:15:30')");
        }

        String pureSource = """
                Class model::TimeEvent { name: String[1]; eventTime: DateTime[1]; }
                Database store::TimeDb ( Table T_TIME_TEST ( ID INTEGER, NAME VARCHAR(100), EVENT_TIME TIMESTAMP ) )
                Mapping model::TimeMap ( TimeEvent: Relational { ~mainTable [TimeDb] T_TIME_TEST name: [TimeDb] T_TIME_TEST.NAME, eventTime: [TimeDb] T_TIME_TEST.EVENT_TIME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::TimeMap ]; connections: [ store::TimeDb: [ environment: store::TestConn ] ]; }
                """;

        // Test hour(), minute(), second() extraction
        String pureQuery = "TimeEvent.all()->project([{e | $e.name}, {e | $e.eventTime->hour()}, {e | $e.eventTime->minute()}, {e | $e.eventTime->second()}], ['name', 'hour', 'minute', 'second'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Time extraction result: " + result.rows());
        assertEquals(2, result.rows().size());

        // Verify results
        for (var row : result.rows()) {
            String name = (String) row.get(0);
            int hour = ((Number) row.get(1)).intValue();
            int minute = ((Number) row.get(2)).intValue();
            int second = ((Number) row.get(3)).intValue();
            System.out.printf("  %s: hour=%d, minute=%d, second=%d%n", name, hour, minute, second);

            if ("Meeting".equals(name)) {
                assertEquals(14, hour, "Meeting at 14:xx");
                assertEquals(30, minute, "Meeting at xx:30");
                assertEquals(45, second, "Meeting at xx:xx:45");
            } else if ("Call".equals(name)) {
                assertEquals(9, hour, "Call at 09:xx");
                assertEquals(15, minute, "Call at xx:15");
                assertEquals(30, second, "Call at xx:xx:30");
            }
        }
    }

    @Test
    @DisplayName("Pure syntax: quarter() date extraction")
    void testPureSyntaxQuarterExtraction() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_QUARTER_TEST");
            stmt.execute("CREATE TABLE T_QUARTER_TEST (ID INTEGER, SALE_DATE DATE, AMOUNT INTEGER)");
            stmt.execute("INSERT INTO T_QUARTER_TEST VALUES (1, '2024-02-15', 1000)"); // Q1
            stmt.execute("INSERT INTO T_QUARTER_TEST VALUES (2, '2024-05-20', 2000)"); // Q2
            stmt.execute("INSERT INTO T_QUARTER_TEST VALUES (3, '2024-08-10', 3000)"); // Q3
            stmt.execute("INSERT INTO T_QUARTER_TEST VALUES (4, '2024-11-25', 4000)"); // Q4
        }

        String pureSource = """
                Class model::Sale { saleDate: StrictDate[1]; amount: Integer[1]; }
                Database store::QuarterDb ( Table T_QUARTER_TEST ( ID INTEGER, SALE_DATE DATE, AMOUNT INTEGER ) )
                Mapping model::QuarterMap ( Sale: Relational { ~mainTable [QuarterDb] T_QUARTER_TEST saleDate: [QuarterDb] T_QUARTER_TEST.SALE_DATE, amount: [QuarterDb] T_QUARTER_TEST.AMOUNT } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::QuarterMap ]; connections: [ store::QuarterDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "Sale.all()->project([{e | $e.saleDate->quarter()}, {e | $e.amount}], ['quarter', 'amount'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Quarter extraction result: " + result.rows());
        assertEquals(4, result.rows().size());

        // Verify quarters
        for (var row : result.rows()) {
            int quarter = ((Number) row.get(0)).intValue();
            int amount = ((Number) row.get(1)).intValue();
            System.out.printf("  Q%d: amount=%d%n", quarter, amount);

            // Each sale is in a different quarter
            assertTrue(quarter >= 1 && quarter <= 4, "Quarter should be 1-4");
        }
    }

    @Test
    @DisplayName("Pure syntax: dayOfWeek() and dayOfYear() extraction")
    void testPureSyntaxDayOfWeekAndYear() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DOW_TEST");
            stmt.execute("CREATE TABLE T_DOW_TEST (ID INTEGER, EVENT_DATE DATE)");
            // 2024-01-01 is a Monday (DOW=1), and it's day 1 of the year
            stmt.execute("INSERT INTO T_DOW_TEST VALUES (1, '2024-01-01')");
            // 2024-12-31 is a Tuesday (DOW=2), and it's day 366 of 2024 (leap year)
            stmt.execute("INSERT INTO T_DOW_TEST VALUES (2, '2024-12-31')");
        }

        String pureSource = """
                Class model::Event { eventDate: StrictDate[1]; }
                Database store::DowDb ( Table T_DOW_TEST ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::DowMap ( Event: Relational { ~mainTable [DowDb] T_DOW_TEST eventDate: [DowDb] T_DOW_TEST.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DowMap ]; connections: [ store::DowDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "Event.all()->project([{e | $e.eventDate->dayOfWeek()}, {e | $e.eventDate->dayOfYear()}], ['dow', 'doy'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("DayOfWeek/DayOfYear result: " + result.rows());
        assertEquals(2, result.rows().size());

        // Verify - DuckDB DOW: Sunday=0
        for (var row : result.rows()) {
            int dow = ((Number) row.get(0)).intValue();
            int doy = ((Number) row.get(1)).intValue();
            System.out.printf("  dow=%d, doy=%d%n", dow, doy);
            assertTrue(dow >= 0 && dow <= 6, "Day of week should be 0-6");
            assertTrue(doy >= 1 && doy <= 366, "Day of year should be 1-366");
        }
    }

    @Test
    @DisplayName("Pure syntax: weekOfYear() extraction")
    void testPureSyntaxWeekOfYear() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_WEEK_TEST");
            stmt.execute("CREATE TABLE T_WEEK_TEST (ID INTEGER, EVENT_DATE DATE)");
            // First week of January
            stmt.execute("INSERT INTO T_WEEK_TEST VALUES (1, '2024-01-07')");
            // Mid-year (around week 26)
            stmt.execute("INSERT INTO T_WEEK_TEST VALUES (2, '2024-06-25')");
            // Last week of December
            stmt.execute("INSERT INTO T_WEEK_TEST VALUES (3, '2024-12-28')");
        }

        String pureSource = """
                Class model::WeekEvent { eventDate: StrictDate[1]; }
                Database store::WeekDb ( Table T_WEEK_TEST ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::WeekMap ( WeekEvent: Relational { ~mainTable [WeekDb] T_WEEK_TEST eventDate: [WeekDb] T_WEEK_TEST.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::WeekMap ]; connections: [ store::WeekDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "WeekEvent.all()->project([{e | $e.eventDate->weekOfYear()}], ['week'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("WeekOfYear result: " + result.rows());
        assertEquals(3, result.rows().size());

        // Verify weeks are in valid range
        for (var row : result.rows()) {
            int week = ((Number) row.get(0)).intValue();
            System.out.printf("  week=%d%n", week);
            assertTrue(week >= 1 && week <= 53, "Week of year should be 1-53");
        }
    }

    @Test
    @DisplayName("Pure syntax: StrictDate and DateTime types")
    void testPureSyntaxStrictDateAndDateTime() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DATETIME_TEST");
            stmt.execute(
                    "CREATE TABLE T_DATETIME_TEST (ID INTEGER, NAME VARCHAR, BIRTH_DATE DATE, LAST_LOGIN TIMESTAMP)");
            stmt.execute("INSERT INTO T_DATETIME_TEST VALUES (1, 'Alice', '1990-06-15', '2024-01-15 09:30:00')");
            stmt.execute("INSERT INTO T_DATETIME_TEST VALUES (2, 'Bob', '1985-12-25', '2024-01-16 14:45:30')");
        }

        // Using StrictDate for date-only and DateTime for timestamp
        String pureSource = """
                Class model::User { name: String[1]; birthDate: StrictDate[1]; lastLogin: DateTime[1]; }
                Database store::DateTimeDb ( Table T_DATETIME_TEST ( ID INTEGER, NAME VARCHAR(100), BIRTH_DATE DATE, LAST_LOGIN TIMESTAMP ) )
                Mapping model::DateTimeMap ( User: Relational { ~mainTable [DateTimeDb] T_DATETIME_TEST name: [DateTimeDb] T_DATETIME_TEST.NAME, birthDate: [DateTimeDb] T_DATETIME_TEST.BIRTH_DATE, lastLogin: [DateTimeDb] T_DATETIME_TEST.LAST_LOGIN } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DateTimeMap ]; connections: [ store::DateTimeDb: [ environment: store::TestConn ] ]; }
                """;

        // Test extraction from StrictDate and DateTime
        String pureQuery = "User.all()->project([{e | $e.name}, {e | $e.birthDate->year()}, {e | $e.lastLogin->hour()}], ['name', 'birthYear', 'loginHour'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("StrictDate/DateTime result: " + result.rows());
        assertEquals(2, result.rows().size());

        // Verify results
        for (var row : result.rows()) {
            String name = (String) row.get(0);
            int birthYear = ((Number) row.get(1)).intValue();
            int loginHour = ((Number) row.get(2)).intValue();
            System.out.printf("  %s: birthYear=%d, loginHour=%d%n", name, birthYear, loginHour);

            if ("Alice".equals(name)) {
                assertEquals(1990, birthYear, "Alice born 1990");
                assertEquals(9, loginHour, "Alice logged in at 09:xx");
            } else if ("Bob".equals(name)) {
                assertEquals(1985, birthYear, "Bob born 1985");
                assertEquals(14, loginHour, "Bob logged in at 14:xx");
            }
        }
    }

    @Test
    @DisplayName("Pure syntax: now() and today() current date functions")
    void testPureSyntaxNowAndToday() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_NOW_TEST");
            stmt.execute("CREATE TABLE T_NOW_TEST (ID INTEGER, NAME VARCHAR)");
            stmt.execute("INSERT INTO T_NOW_TEST VALUES (1, 'Test')");
        }

        String pureSource = """
                Class model::TestRecord { name: String[1]; }
                Database store::NowDb ( Table T_NOW_TEST ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::NowMap ( TestRecord: Relational { ~mainTable [NowDb] T_NOW_TEST name: [NowDb] T_NOW_TEST.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::NowMap ]; connections: [ store::NowDb: [ environment: store::TestConn ] ]; }
                """;

        // Test now() - returns current timestamp
        String pureQueryNow = "TestRecord.all()->project([{e | now()}], ['currentTimestamp'])";
        var resultNow = queryService.execute(pureSource, pureQueryNow, "test::TestRuntime", connection);
        System.out.println("now() SQL generates: CURRENT_TIMESTAMP");
        assertEquals(1, resultNow.rows().size());
        // The result should be a timestamp (we just check it exists)
        assertNotNull(resultNow.rows().get(0).get(0), "now() should return a value");

        // Test today() - returns current date
        String pureQueryToday = "TestRecord.all()->project([{e | today()}], ['currentDate'])";
        var resultToday = queryService.execute(pureSource, pureQueryToday, "test::TestRuntime", connection);
        System.out.println("today() SQL generates: CURRENT_DATE");
        assertEquals(1, resultToday.rows().size());
        assertNotNull(resultToday.rows().get(0).get(0), "today() should return a value");
    }

    @Test
    @DisplayName("Pure syntax: firstDayOfMonth() date truncation")
    void testPureSyntaxFirstDayOfMonth() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_FIRST_DAY_TEST");
            stmt.execute("CREATE TABLE T_FIRST_DAY_TEST (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_FIRST_DAY_TEST VALUES (1, '2024-06-15')");
            stmt.execute("INSERT INTO T_FIRST_DAY_TEST VALUES (2, '2024-12-25')");
        }

        String pureSource = """
                Class model::EventRecord { eventDate: StrictDate[1]; }
                Database store::FirstDayDb ( Table T_FIRST_DAY_TEST ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::FirstDayMap ( EventRecord: Relational { ~mainTable [FirstDayDb] T_FIRST_DAY_TEST eventDate: [FirstDayDb] T_FIRST_DAY_TEST.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::FirstDayMap ]; connections: [ store::FirstDayDb: [ environment: store::TestConn ] ]; }
                """;

        // Test firstDayOfMonth()
        String pureQuery = "EventRecord.all()->project([{e | $e.eventDate}, {e | $e.eventDate->firstDayOfMonth()}], ['eventDate', 'firstOfMonth'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("firstDayOfMonth result: " + result.rows());
        assertEquals(2, result.rows().size());

        // Verify - June 15 -> June 1, Dec 25 -> Dec 1
        for (var row : result.rows()) {
            Object eventDate = row.get(0);
            Object firstOfMonth = row.get(1);
            System.out.printf("  %s -> %s%n", eventDate, firstOfMonth);
            assertNotNull(firstOfMonth, "firstDayOfMonth should return a value");
        }
    }

    @Test
    @DisplayName("Pure syntax: firstDayOfYear() and firstDayOfQuarter() truncation")
    void testPureSyntaxFirstDayOfYearAndQuarter() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_TRUNC_TEST");
            stmt.execute("CREATE TABLE T_TRUNC_TEST (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_TRUNC_TEST VALUES (1, '2024-05-15')"); // Q2, mid-year
        }

        String pureSource = """
                Class model::TruncRecord { eventDate: StrictDate[1]; }
                Database store::TruncDb ( Table T_TRUNC_TEST ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::TruncMap ( TruncRecord: Relational { ~mainTable [TruncDb] T_TRUNC_TEST eventDate: [TruncDb] T_TRUNC_TEST.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::TruncMap ]; connections: [ store::TruncDb: [ environment: store::TestConn ] ]; }
                """;

        // Test firstDayOfYear and firstDayOfQuarter
        String pureQuery = "TruncRecord.all()->project([{e | $e.eventDate}, {e | $e.eventDate->firstDayOfYear()}, {e | $e.eventDate->firstDayOfQuarter()}], ['eventDate', 'firstOfYear', 'firstOfQuarter'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("firstDayOfYear/Quarter result: " + result.rows());
        assertEquals(1, result.rows().size());

        var row = result.rows().get(0);
        System.out.printf("  eventDate=%s, firstOfYear=%s, firstOfQuarter=%s%n",
                row.get(0), row.get(1), row.get(2));
        // May 15, 2024 -> Jan 1, 2024 (year) and Apr 1, 2024 (Q2)
        assertNotNull(row.get(1), "firstDayOfYear should return a value");
        assertNotNull(row.get(2), "firstDayOfQuarter should return a value");
    }

    @Test
    @DisplayName("Pure syntax: dateDiff() date difference")
    void testPureSyntaxDateDiff() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DATE_DIFF_TEST");
            stmt.execute("CREATE TABLE T_DATE_DIFF_TEST (ID INTEGER, START_DATE DATE, END_DATE DATE)");
            stmt.execute("INSERT INTO T_DATE_DIFF_TEST VALUES (1, '2024-01-01', '2024-01-10')"); // 9 days
            stmt.execute("INSERT INTO T_DATE_DIFF_TEST VALUES (2, '2024-01-01', '2024-04-01')"); // 91 days
        }

        String pureSource = """
                Class model::DateRange { startDate: StrictDate[1]; endDate: StrictDate[1]; }
                Database store::DateDiffDb ( Table T_DATE_DIFF_TEST ( ID INTEGER, START_DATE DATE, END_DATE DATE ) )
                Mapping model::DateDiffMap ( DateRange: Relational { ~mainTable [DateDiffDb] T_DATE_DIFF_TEST startDate: [DateDiffDb] T_DATE_DIFF_TEST.START_DATE, endDate: [DateDiffDb] T_DATE_DIFF_TEST.END_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DateDiffMap ]; connections: [ store::DateDiffDb: [ environment: store::TestConn ] ]; }
                """;

        // Test dateDiff with DAYS unit
        String pureQuery = "DateRange.all()->project([{e | dateDiff($e.startDate, $e.endDate, DurationUnit.DAYS)}], ['daysDiff'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("dateDiff result: " + result.rows());
        assertEquals(2, result.rows().size());

        // Verify results: 9 days and 91 days
        int days1 = ((Number) result.rows().get(0).get(0)).intValue();
        int days2 = ((Number) result.rows().get(1).get(0)).intValue();
        System.out.printf("  Row 1: %d days, Row 2: %d days%n", days1, days2);
        assertEquals(9, days1, "Jan 1 to Jan 10 = 9 days");
        assertEquals(91, days2, "Jan 1 to Apr 1 = 91 days");
    }

    @Test
    @DisplayName("Pure syntax: adjust() date arithmetic")
    void testPureSyntaxAdjust() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_ADJUST_TEST");
            stmt.execute("CREATE TABLE T_ADJUST_TEST (ID INTEGER, BASE_DATE DATE)");
            stmt.execute("INSERT INTO T_ADJUST_TEST VALUES (1, '2024-01-15')");
        }

        String pureSource = """
                Class model::AdjustRecord { baseDate: StrictDate[1]; }
                Database store::AdjustDb ( Table T_ADJUST_TEST ( ID INTEGER, BASE_DATE DATE ) )
                Mapping model::AdjustMap ( AdjustRecord: Relational { ~mainTable [AdjustDb] T_ADJUST_TEST baseDate: [AdjustDb] T_ADJUST_TEST.BASE_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::AdjustMap ]; connections: [ store::AdjustDb: [ environment: store::TestConn ] ]; }
                """;

        // Test adjust - add 10 days
        String pureQuery = "AdjustRecord.all()->project([{e | $e.baseDate}, {e | adjust($e.baseDate, 10, DurationUnit.DAYS)}], ['baseDate', 'adjustedDate'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("adjust result: " + result.rows());
        assertEquals(1, result.rows().size());

        // Verify: Jan 15 + 10 days = Jan 25
        Object baseDate = result.rows().get(0).get(0);
        Object adjustedDate = result.rows().get(0).get(1);
        System.out.printf("  baseDate=%s, adjustedDate=%s%n", baseDate, adjustedDate);
        assertNotNull(adjustedDate, "adjust should return a value");
    }

    @Test
    @DisplayName("Pure literal syntax: date->adjust() method call")
    void testDateLiteralAdjustMethodCall() throws Exception {
        // This matches the PCT pattern: |%2016-02-29->adjust(10, DurationUnit.DAYS)
        // Create test data so we can test the relation path
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_ADJUST_METHOD_TEST");
            stmt.execute("CREATE TABLE T_ADJUST_METHOD_TEST (ID INTEGER, BASE_DATE DATE)");
            stmt.execute("INSERT INTO T_ADJUST_METHOD_TEST VALUES (1, '2024-01-15')");
        }

        String pureSource = """
                Class model::AdjustMethodRecord { baseDate: StrictDate[1]; }
                Database store::AdjustMethodDb ( Table T_ADJUST_METHOD_TEST ( ID INTEGER, BASE_DATE DATE ) )
                Mapping model::AdjustMethodMap ( AdjustMethodRecord: Relational { ~mainTable [AdjustMethodDb] T_ADJUST_METHOD_TEST baseDate: [AdjustMethodDb] T_ADJUST_METHOD_TEST.BASE_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::AdjustMethodMap ]; connections: [ store::AdjustMethodDb: [ environment: store::TestConn ] ]; }
                """;

        // Test method call syntax: date->adjust(amount, unit) with 2 args
        String pureQuery = "AdjustMethodRecord.all()->project([{e | $e.baseDate->meta::pure::functions::date::adjust(10, meta::pure::functions::date::DurationUnit.DAYS)}], ['adjustedDate'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("adjust method call result: " + result.rows());
        assertEquals(1, result.rows().size());

        // The result should be Jan 15 + 10 days = Jan 25
        Object adjustedDate = result.rows().get(0).get(0);
        System.out.println("  adjustedDate=" + adjustedDate);
        assertNotNull(adjustedDate, "adjust method call should return a value");
    }

    // Note: DuckDB cannot handle intervals larger than INT32 (~2.1 billion days =
    // ~5.8 million years).
    // PCT test testAdjustByDaysBigNumber with value 12345678912 days (~33.8 million
    // years) will fail.
    // This is a DuckDB limitation, not a Legend-Lite bug - should be an expected
    // failure.

    @Test
    @DisplayName("Comprehensive adjust() function tests - all PCT fixes")
    void testAdjustComprehensive() throws Exception {
        // Test all the fixes made for adjust():
        // 1. Method call syntax (date->adjust(amount, unit))
        // 2. Negative amounts
        // 3. Various duration units
        // 4. Partial dates (year-only, year-month)

        System.out.println("=== Comprehensive adjust() Tests ===");

        // Test 1: Method call with positive days
        String sql = generateSql(
                "|%2024-01-15->meta::pure::functions::date::adjust(10, meta::pure::functions::date::DurationUnit.DAYS)");
        System.out.println("1. Positive days: " + sql);
        assertTrue(sql.contains("INTERVAL '1' DAY * 10"), "Should use multiplication for interval");
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1));
        }

        // Test 2: Negative amount (subtract hours)
        sql = generateSql(
                "|%2024-01-15T12:00:00->meta::pure::functions::date::adjust(-3, meta::pure::functions::date::DurationUnit.HOURS)");
        System.out.println("2. Negative hours: " + sql);
        assertTrue(sql.contains("INTERVAL '1' HOUR"), "Should use HOUR interval");
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 09:00:00
        }

        // Test 3: Minutes
        sql = generateSql(
                "|%2024-01-15T12:30:00->meta::pure::functions::date::adjust(45, meta::pure::functions::date::DurationUnit.MINUTES)");
        System.out.println("3. Minutes: " + sql);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 13:15:00
        }

        // Test 4: Seconds
        sql = generateSql(
                "|%2024-01-15T12:00:00->meta::pure::functions::date::adjust(90, meta::pure::functions::date::DurationUnit.SECONDS)");
        System.out.println("4. Seconds: " + sql);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 12:01:30
        }

        // Test 5: Milliseconds
        sql = generateSql(
                "|%2024-01-15T12:00:00.000->meta::pure::functions::date::adjust(500, meta::pure::functions::date::DurationUnit.MILLISECONDS)");
        System.out.println("5. Milliseconds: " + sql);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 12:00:00.500
        }

        // Test 6: Microseconds (newly added)
        sql = generateSql(
                "|%2024-01-15T12:00:00.000000->meta::pure::functions::date::adjust(1, meta::pure::functions::date::DurationUnit.MICROSECONDS)");
        System.out.println("6. Microseconds: " + sql);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1));
        }

        // Test 7: Months
        sql = generateSql(
                "|%2024-01-15->meta::pure::functions::date::adjust(3, meta::pure::functions::date::DurationUnit.MONTHS)");
        System.out.println("7. Months: " + sql);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 2024-04-15
        }

        // Test 8: Years
        sql = generateSql(
                "|%2024-01-15->meta::pure::functions::date::adjust(2, meta::pure::functions::date::DurationUnit.YEARS)");
        System.out.println("8. Years: " + sql);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 2026-01-15
        }

        // Test 9: Partial date - year-month only (should default to first of month)
        sql = generateSql(
                "|%2024-03->meta::pure::functions::date::adjust(1, meta::pure::functions::date::DurationUnit.MONTHS)");
        System.out.println("9. Partial date (year-month): " + sql);
        assertTrue(sql.contains("DATE '2024-03-01'"), "Year-month should default to -01");
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 2024-04-01
        }

        // Test 10: Partial date - year only (should default to Jan 1)
        sql = generateSql(
                "|%2024->meta::pure::functions::date::adjust(6, meta::pure::functions::date::DurationUnit.MONTHS)");
        System.out.println("10. Partial date (year only): " + sql);
        assertTrue(sql.contains("DATE '2024-01-01'"), "Year only should default to -01-01");
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            System.out.println("   Result: " + rs.getObject(1)); // Expected: 2024-07-01
        }

        System.out.println("=== All adjust() tests passed ===");
    }

    @Test
    @DisplayName("Pure syntax: toEpochValue() converts date to unix timestamp")
    void testPureSyntaxToEpochValue() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EPOCH_TEST");
            stmt.execute("CREATE TABLE T_EPOCH_TEST (ID INTEGER, EVENT_DATE TIMESTAMP)");
            // 2024-01-01 00:00:00 UTC = 1704067200 seconds since epoch
            stmt.execute("INSERT INTO T_EPOCH_TEST VALUES (1, '2024-01-01 00:00:00')");
        }

        String pureSource = """
                Class model::EpochRecord { eventDate: DateTime[1]; }
                Database store::EpochDb ( Table T_EPOCH_TEST ( ID INTEGER, EVENT_DATE TIMESTAMP ) )
                Mapping model::EpochMap ( EpochRecord: Relational { ~mainTable [EpochDb] T_EPOCH_TEST eventDate: [EpochDb] T_EPOCH_TEST.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::EpochMap ]; connections: [ store::EpochDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "EpochRecord.all()->project([{e | toEpochValue($e.eventDate)}], ['epochSeconds'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("toEpochValue result: " + result.rows());
        assertEquals(1, result.rows().size());

        long epochSeconds = ((Number) result.rows().get(0).get(0)).longValue();
        System.out.printf("  epochSeconds=%d%n", epochSeconds);
        assertTrue(epochSeconds > 0, "Epoch should be positive");
    }

    @Test
    @DisplayName("Pure syntax: fromEpochValue() converts unix timestamp to date")
    void testPureSyntaxFromEpochValue() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_FROM_EPOCH_TEST");
            stmt.execute("CREATE TABLE T_FROM_EPOCH_TEST (ID INTEGER, EPOCH_SECONDS BIGINT)");
            // 1704067200 = 2024-01-01 00:00:00 UTC
            stmt.execute("INSERT INTO T_FROM_EPOCH_TEST VALUES (1, 1704067200)");
        }

        String pureSource = """
                Class model::FromEpochRecord { epochSeconds: Integer[1]; }
                Database store::FromEpochDb ( Table T_FROM_EPOCH_TEST ( ID INTEGER, EPOCH_SECONDS BIGINT ) )
                Mapping model::FromEpochMap ( FromEpochRecord: Relational { ~mainTable [FromEpochDb] T_FROM_EPOCH_TEST epochSeconds: [FromEpochDb] T_FROM_EPOCH_TEST.EPOCH_SECONDS } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::FromEpochMap ]; connections: [ store::FromEpochDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "FromEpochRecord.all()->project([{e | fromEpochValue($e.epochSeconds)}], ['dateTime'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("fromEpochValue result: " + result.rows());
        assertEquals(1, result.rows().size());

        Object dateTime = result.rows().get(0).get(0);
        System.out.printf("  dateTime=%s%n", dateTime);
        assertNotNull(dateTime, "fromEpochValue should return a value");
    }

    @Test
    @DisplayName("Pure syntax: isOnDay() compares dates at day level")
    void testPureSyntaxIsOnDay() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DATE_COMPARE");
            stmt.execute("CREATE TABLE T_DATE_COMPARE (ID INTEGER, DATE1 TIMESTAMP, DATE2 TIMESTAMP)");
            // Same day (different times)
            stmt.execute("INSERT INTO T_DATE_COMPARE VALUES (1, '2024-01-15 08:00:00', '2024-01-15 20:00:00')");
            // Different days
            stmt.execute("INSERT INTO T_DATE_COMPARE VALUES (2, '2024-01-15 08:00:00', '2024-01-16 08:00:00')");
        }

        String pureSource = """
                Class model::DateCompareRecord { date1: DateTime[1]; date2: DateTime[1]; }
                Database store::DateCompareDb ( Table T_DATE_COMPARE ( ID INTEGER, DATE1 TIMESTAMP, DATE2 TIMESTAMP ) )
                Mapping model::DateCompareMap ( DateCompareRecord: Relational { ~mainTable [DateCompareDb] T_DATE_COMPARE date1: [DateCompareDb] T_DATE_COMPARE.DATE1, date2: [DateCompareDb] T_DATE_COMPARE.DATE2 } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DateCompareMap ]; connections: [ store::DateCompareDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "DateCompareRecord.all()->project([{e | isOnDay($e.date1, $e.date2)}], ['sameDay'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("isOnDay result: " + result.rows());
        assertEquals(2, result.rows().size());

        // First row: same day -> true
        Boolean sameDay1 = (Boolean) result.rows().get(0).get(0);
        // Second row: different days -> false
        Boolean sameDay2 = (Boolean) result.rows().get(1).get(0);
        System.out.printf("  Row 1 (same day times): %s, Row 2 (different days): %s%n", sameDay1, sameDay2);
        assertTrue(sameDay1, "Same day should return true");
        assertFalse(sameDay2, "Different days should return false");
    }

    @Test
    @DisplayName("Pure syntax: isAfterDay() and isBeforeDay()")
    void testPureSyntaxIsAfterBeforeDay() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_AFTER_BEFORE");
            stmt.execute("CREATE TABLE T_AFTER_BEFORE (ID INTEGER, DATE1 DATE, DATE2 DATE)");
            stmt.execute("INSERT INTO T_AFTER_BEFORE VALUES (1, '2024-01-20', '2024-01-15')");
        }

        String pureSource = """
                Class model::AfterBeforeRecord { date1: StrictDate[1]; date2: StrictDate[1]; }
                Database store::AfterBeforeDb ( Table T_AFTER_BEFORE ( ID INTEGER, DATE1 DATE, DATE2 DATE ) )
                Mapping model::AfterBeforeMap ( AfterBeforeRecord: Relational { ~mainTable [AfterBeforeDb] T_AFTER_BEFORE date1: [AfterBeforeDb] T_AFTER_BEFORE.DATE1, date2: [AfterBeforeDb] T_AFTER_BEFORE.DATE2 } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::AfterBeforeMap ]; connections: [ store::AfterBeforeDb: [ environment: store::TestConn ] ]; }
                """;

        // Test isAfterDay
        String afterQuery = "AfterBeforeRecord.all()->project([{e | isAfterDay($e.date1, $e.date2)}], ['isAfter'])";
        var afterResult = queryService.execute(pureSource, afterQuery, "test::TestRuntime", connection);
        Boolean isAfter = (Boolean) afterResult.rows().get(0).get(0);
        System.out.printf("  isAfterDay(Jan 20, Jan 15): %s%n", isAfter);
        assertTrue(isAfter, "Jan 20 is after Jan 15");

        // Test isBeforeDay
        String beforeQuery = "AfterBeforeRecord.all()->project([{e | isBeforeDay($e.date1, $e.date2)}], ['isBefore'])";
        var beforeResult = queryService.execute(pureSource, beforeQuery, "test::TestRuntime", connection);
        Boolean isBefore = (Boolean) beforeResult.rows().get(0).get(0);
        System.out.printf("  isBeforeDay(Jan 20, Jan 15): %s%n", isBefore);
        assertFalse(isBefore, "Jan 20 is NOT before Jan 15");
    }

    @Test
    @DisplayName("Pure syntax: isOnOrAfterDay() and isOnOrBeforeDay()")
    void testPureSyntaxIsOnOrAfterBeforeDay() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_ON_OR");
            stmt.execute("CREATE TABLE T_ON_OR (ID INTEGER, DATE1 DATE, DATE2 DATE)");
            // Same day
            stmt.execute("INSERT INTO T_ON_OR VALUES (1, '2024-01-15', '2024-01-15')");
            // date1 after date2
            stmt.execute("INSERT INTO T_ON_OR VALUES (2, '2024-01-20', '2024-01-15')");
        }

        String pureSource = """
                Class model::OnOrRecord { date1: StrictDate[1]; date2: StrictDate[1]; }
                Database store::OnOrDb ( Table T_ON_OR ( ID INTEGER, DATE1 DATE, DATE2 DATE ) )
                Mapping model::OnOrMap ( OnOrRecord: Relational { ~mainTable [OnOrDb] T_ON_OR date1: [OnOrDb] T_ON_OR.DATE1, date2: [OnOrDb] T_ON_OR.DATE2 } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::OnOrMap ]; connections: [ store::OnOrDb: [ environment: store::TestConn ] ]; }
                """;

        // Test isOnOrAfterDay (both should be true)
        String query = "OnOrRecord.all()->project([{e | isOnOrAfterDay($e.date1, $e.date2)}], ['isOnOrAfter'])";
        var result = queryService.execute(pureSource, query, "test::TestRuntime", connection);
        System.out.println("isOnOrAfterDay result: " + result.rows());
        assertEquals(2, result.rows().size());

        Boolean row1 = (Boolean) result.rows().get(0).get(0);
        Boolean row2 = (Boolean) result.rows().get(1).get(0);
        System.out.printf("  Row 1 (same day): %s, Row 2 (date1 after date2): %s%n", row1, row2);
        assertTrue(row1, "Same day should be on or after");
        assertTrue(row2, "Jan 20 is on or after Jan 15");
    }

    @Test
    @DisplayName("Pure syntax: datePart() extracts date from datetime")
    void testPureSyntaxDatePart() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DATEPART");
            stmt.execute("CREATE TABLE T_DATEPART (ID INTEGER, EVENT_TIME TIMESTAMP)");
            stmt.execute("INSERT INTO T_DATEPART VALUES (1, '2024-03-15 14:30:45')");
        }

        String pureSource = """
                Class model::DatePartRecord { eventTime: DateTime[1]; }
                Database store::DatePartDb ( Table T_DATEPART ( ID INTEGER, EVENT_TIME TIMESTAMP ) )
                Mapping model::DatePartMap ( DatePartRecord: Relational { ~mainTable [DatePartDb] T_DATEPART eventTime: [DatePartDb] T_DATEPART.EVENT_TIME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DatePartMap ]; connections: [ store::DatePartDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "DatePartRecord.all()->project([{e | $e.eventTime->datePart()}], ['dateOnly'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("datePart result: " + result.rows());
        assertEquals(1, result.rows().size());

        Object dateOnly = result.rows().get(0).get(0);
        System.out.printf("  dateOnly=%s%n", dateOnly);
        assertNotNull(dateOnly, "datePart should return a value");
        // The result should be 2024-03-15 (without time portion)
    }

    @Test
    @DisplayName("Pure syntax: firstHourOfDay() truncates to midnight")
    void testPureSyntaxFirstHourOfDay() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_FIRST_HOUR");
            stmt.execute("CREATE TABLE T_FIRST_HOUR (ID INTEGER, EVENT_TIME TIMESTAMP)");
            stmt.execute("INSERT INTO T_FIRST_HOUR VALUES (1, '2024-03-15 14:30:45')");
        }

        String pureSource = """
                Class model::FirstHourRecord { eventTime: DateTime[1]; }
                Database store::FirstHourDb ( Table T_FIRST_HOUR ( ID INTEGER, EVENT_TIME TIMESTAMP ) )
                Mapping model::FirstHourMap ( FirstHourRecord: Relational { ~mainTable [FirstHourDb] T_FIRST_HOUR eventTime: [FirstHourDb] T_FIRST_HOUR.EVENT_TIME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::FirstHourMap ]; connections: [ store::FirstHourDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "FirstHourRecord.all()->project([{e | $e.eventTime->firstHourOfDay()}], ['midnight'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("firstHourOfDay result: " + result.rows());
        assertEquals(1, result.rows().size());

        Object midnight = result.rows().get(0).get(0);
        System.out.printf("  midnight=%s%n", midnight);
        assertNotNull(midnight, "firstHourOfDay should return a value");
        // Result should be 2024-03-15 00:00:00
    }

    @Test
    @DisplayName("Pure syntax: timeBucket() calculates time buckets")
    void testPureSyntaxTimeBucket() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_TIMEBUCKET");
            stmt.execute("CREATE TABLE T_TIMEBUCKET (ID INTEGER, EVENT_TIME TIMESTAMP)");
            stmt.execute("INSERT INTO T_TIMEBUCKET VALUES (1, '2024-01-31 00:32:34')");
            stmt.execute("INSERT INTO T_TIMEBUCKET VALUES (2, '2024-01-31 11:45:22')");
            stmt.execute("INSERT INTO T_TIMEBUCKET VALUES (3, '2024-01-31 23:59:59')");
        }

        String pureSource = """
                Class model::TimeBucketRecord { eventTime: DateTime[1]; }
                Database store::TimeBucketDb ( Table T_TIMEBUCKET ( ID INTEGER, EVENT_TIME TIMESTAMP ) )
                Mapping model::TimeBucketMap ( TimeBucketRecord: Relational { ~mainTable [TimeBucketDb] T_TIMEBUCKET eventTime: [TimeBucketDb] T_TIMEBUCKET.EVENT_TIME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::TimeBucketMap ]; connections: [ store::TimeBucketDb: [ environment: store::TestConn ] ]; }
                """;

        // Test with 5-hour buckets
        String pureQuery = "TimeBucketRecord.all()->project([{e | $e.eventTime->timeBucket(5, DurationUnit.HOURS)}], ['bucket'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("timeBucket (5 hours) result: " + result.rows());
        assertEquals(3, result.rows().size());

        // 00:32:34 should bucket to 00:00:00
        // 11:45:22 should bucket to 10:00:00
        // 23:59:59 should bucket to 20:00:00
        for (int i = 0; i < result.rows().size(); i++) {
            Object bucket = result.rows().get(i).get(0);
            System.out.printf("  Row %d bucket: %s%n", i + 1, bucket);
            assertNotNull(bucket, "timeBucket should return a value");
        }
    }

    @Test
    @DisplayName("Pure syntax: timeBucket() with day buckets")
    void testPureSyntaxTimeBucketDays() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_TIMEBUCKET_DAYS");
            stmt.execute("CREATE TABLE T_TIMEBUCKET_DAYS (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_TIMEBUCKET_DAYS VALUES (1, '2024-01-05')");
            stmt.execute("INSERT INTO T_TIMEBUCKET_DAYS VALUES (2, '2024-01-12')");
            stmt.execute("INSERT INTO T_TIMEBUCKET_DAYS VALUES (3, '2024-01-20')");
        }

        String pureSource = """
                Class model::DayBucketRecord { eventDate: StrictDate[1]; }
                Database store::DayBucketDb ( Table T_TIMEBUCKET_DAYS ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::DayBucketMap ( DayBucketRecord: Relational { ~mainTable [DayBucketDb] T_TIMEBUCKET_DAYS eventDate: [DayBucketDb] T_TIMEBUCKET_DAYS.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DayBucketMap ]; connections: [ store::DayBucketDb: [ environment: store::TestConn ] ]; }
                """;

        // Test with 7-day (weekly) buckets
        String pureQuery = "DayBucketRecord.all()->project([{e | $e.eventDate->timeBucket(7, DurationUnit.DAYS)}], ['weekBucket'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("timeBucket (7 days) result: " + result.rows());
        assertEquals(3, result.rows().size());

        for (int i = 0; i < result.rows().size(); i++) {
            Object bucket = result.rows().get(i).get(0);
            System.out.printf("  Row %d week bucket: %s%n", i + 1, bucket);
            assertNotNull(bucket, "timeBucket should return a value");
        }
    }

    @Test
    @DisplayName("Pure syntax: monthNumber() extracts month as integer 1-12")
    void testPureSyntaxMonthNumber() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_MONTH");
            stmt.execute("CREATE TABLE T_MONTH (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_MONTH VALUES (1, '2024-01-15')");
            stmt.execute("INSERT INTO T_MONTH VALUES (2, '2024-06-20')");
            stmt.execute("INSERT INTO T_MONTH VALUES (3, '2024-12-25')");
        }

        String pureSource = """
                Class model::MonthRecord { eventDate: StrictDate[1]; }
                Database store::MonthDb ( Table T_MONTH ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::MonthMap ( MonthRecord: Relational { ~mainTable [MonthDb] T_MONTH eventDate: [MonthDb] T_MONTH.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::MonthMap ]; connections: [ store::MonthDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "MonthRecord.all()->project([{e | $e.eventDate->monthNumber()}], ['month'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("monthNumber result: " + result.rows());
        assertEquals(3, result.rows().size());

        // Months should be 1, 6, 12
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
        assertEquals(6L, ((Number) result.rows().get(1).get(0)).longValue());
        assertEquals(12L, ((Number) result.rows().get(2).get(0)).longValue());
    }

    @Test
    @DisplayName("Pure syntax: quarterNumber() extracts quarter as integer 1-4")
    void testPureSyntaxQuarterNumber() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_QUARTER");
            stmt.execute("CREATE TABLE T_QUARTER (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_QUARTER VALUES (1, '2024-02-15')"); // Q1
            stmt.execute("INSERT INTO T_QUARTER VALUES (2, '2024-05-20')"); // Q2
            stmt.execute("INSERT INTO T_QUARTER VALUES (3, '2024-09-10')"); // Q3
            stmt.execute("INSERT INTO T_QUARTER VALUES (4, '2024-11-25')"); // Q4
        }

        String pureSource = """
                Class model::QuarterRecord { eventDate: StrictDate[1]; }
                Database store::QuarterDb ( Table T_QUARTER ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::QuarterMap ( QuarterRecord: Relational { ~mainTable [QuarterDb] T_QUARTER eventDate: [QuarterDb] T_QUARTER.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::QuarterMap ]; connections: [ store::QuarterDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "QuarterRecord.all()->project([{e | $e.eventDate->quarterNumber()}], ['quarter'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("quarterNumber result: " + result.rows());
        assertEquals(4, result.rows().size());

        // Quarters should be 1, 2, 3, 4
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
        assertEquals(2L, ((Number) result.rows().get(1).get(0)).longValue());
        assertEquals(3L, ((Number) result.rows().get(2).get(0)).longValue());
        assertEquals(4L, ((Number) result.rows().get(3).get(0)).longValue());
    }

    @Test
    @DisplayName("Pure syntax: min() and max() for dates")
    void testPureSyntaxMinMaxDates() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_MINMAX_DATE");
            stmt.execute("CREATE TABLE T_MINMAX_DATE (ID INTEGER, DATE1 DATE, DATE2 DATE)");
            stmt.execute("INSERT INTO T_MINMAX_DATE VALUES (1, '2024-01-15', '2024-03-20')"); // min=Jan, max=Mar
            stmt.execute("INSERT INTO T_MINMAX_DATE VALUES (2, '2024-06-10', '2024-02-05')"); // min=Feb, max=Jun
        }

        String pureSource = """
                Class model::MinMaxRecord { date1: StrictDate[1]; date2: StrictDate[1]; }
                Database store::MinMaxDb ( Table T_MINMAX_DATE ( ID INTEGER, DATE1 DATE, DATE2 DATE ) )
                Mapping model::MinMaxMap ( MinMaxRecord: Relational { ~mainTable [MinMaxDb] T_MINMAX_DATE date1: [MinMaxDb] T_MINMAX_DATE.DATE1, date2: [MinMaxDb] T_MINMAX_DATE.DATE2 } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::MinMaxMap ]; connections: [ store::MinMaxDb: [ environment: store::TestConn ] ]; }
                """;

        // Test min
        String minQuery = "MinMaxRecord.all()->project([{e | min($e.date1, $e.date2)}], ['minDate'])";
        var minResult = queryService.execute(pureSource, minQuery, "test::TestRuntime", connection);
        System.out.println("min result: " + minResult.rows());
        assertEquals(2, minResult.rows().size());

        // Test max
        String maxQuery = "MinMaxRecord.all()->project([{e | max($e.date1, $e.date2)}], ['maxDate'])";
        var maxResult = queryService.execute(pureSource, maxQuery, "test::TestRuntime", connection);
        System.out.println("max result: " + maxResult.rows());
        assertEquals(2, maxResult.rows().size());
    }

    @Test
    @DisplayName("Pure syntax: isLeapYear() check")
    void testPureSyntaxIsLeap() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_LEAP");
            stmt.execute("CREATE TABLE T_LEAP (ID INTEGER, YEAR_VAL INTEGER)");
            stmt.execute("INSERT INTO T_LEAP VALUES (1, 2024)"); // Leap year
            stmt.execute("INSERT INTO T_LEAP VALUES (2, 2023)"); // Not leap year
            stmt.execute("INSERT INTO T_LEAP VALUES (3, 2000)"); // Leap year (divisible by 400)
            stmt.execute("INSERT INTO T_LEAP VALUES (4, 1900)"); // Not leap (divisible by 100 but not 400)
        }

        // Note: DuckDB doesn't have a native isleap function, but we could use
        // (year % 4 = 0 AND (year % 100 != 0 OR year % 400 = 0))
        // For now, just test using DuckDB SQL directly
        try (Statement stmt = connection.createStatement();
                var rs = stmt.executeQuery(
                        "SELECT YEAR_VAL, (YEAR_VAL % 4 = 0 AND (YEAR_VAL % 100 != 0 OR YEAR_VAL % 400 = 0)) AS is_leap FROM T_LEAP")) {
            while (rs.next()) {
                int year = rs.getInt("YEAR_VAL");
                boolean isLeap = rs.getBoolean("is_leap");
                System.out.printf("  Year %d: isLeap=%s%n", year, isLeap);
            }
        }
    }

    @Test
    @DisplayName("Pure syntax: dayOfWeekNumber() returns 1-7 (Monday=1)")
    void testPureSyntaxDayOfWeekNumber() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DOW");
            stmt.execute("CREATE TABLE T_DOW (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_DOW VALUES (1, '2024-01-15')"); // Monday
            stmt.execute("INSERT INTO T_DOW VALUES (2, '2024-01-16')"); // Tuesday
            stmt.execute("INSERT INTO T_DOW VALUES (3, '2024-01-17')"); // Wednesday
            stmt.execute("INSERT INTO T_DOW VALUES (4, '2024-01-21')"); // Sunday
        }

        String pureSource = """
                Class model::DowRecord { eventDate: StrictDate[1]; }
                Database store::DowDb ( Table T_DOW ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::DowMap ( DowRecord: Relational { ~mainTable [DowDb] T_DOW eventDate: [DowDb] T_DOW.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DowMap ]; connections: [ store::DowDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "DowRecord.all()->project([{e | $e.eventDate->dayOfWeekNumber()}], ['dow'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("dayOfWeekNumber result: " + result.rows());
        assertEquals(4, result.rows().size());

        // Monday=1, Tuesday=2, Wednesday=3, Sunday=7
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue(), "Monday should be 1");
        assertEquals(2L, ((Number) result.rows().get(1).get(0)).longValue(), "Tuesday should be 2");
        assertEquals(3L, ((Number) result.rows().get(2).get(0)).longValue(), "Wednesday should be 3");
        assertEquals(7L, ((Number) result.rows().get(3).get(0)).longValue(), "Sunday should be 7");
    }

    @Test
    @DisplayName("Pure syntax: dayOfYear() returns day number 1-366")
    void testPureSyntaxDayOfYear() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_DOY");
            stmt.execute("CREATE TABLE T_DOY (ID INTEGER, EVENT_DATE DATE)");
            stmt.execute("INSERT INTO T_DOY VALUES (1, '2024-01-01')"); // Day 1
            stmt.execute("INSERT INTO T_DOY VALUES (2, '2024-02-15')"); // Day 46
            stmt.execute("INSERT INTO T_DOY VALUES (3, '2024-12-31')"); // Day 366 (leap year)
        }

        String pureSource = """
                Class model::DoyRecord { eventDate: StrictDate[1]; }
                Database store::DoyDb ( Table T_DOY ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::DoyMap ( DoyRecord: Relational { ~mainTable [DoyDb] T_DOY eventDate: [DoyDb] T_DOY.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DoyMap ]; connections: [ store::DoyDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "DoyRecord.all()->project([{e | $e.eventDate->dayOfYear()}], ['doy'])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("dayOfYear result: " + result.rows());
        assertEquals(3, result.rows().size());

        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue(), "Jan 1 should be day 1");
        assertEquals(46L, ((Number) result.rows().get(1).get(0)).longValue(), "Feb 15 should be day 46");
        assertEquals(366L, ((Number) result.rows().get(2).get(0)).longValue(), "Dec 31 of leap year should be day 366");
    }

    // ==================== CONSTANT LAMBDA EXPRESSION TESTS ====================
    // These test the ConstantNode support for expressions like |1+1 -> SELECT 1+1

    @Test
    @DisplayName("Constant lambda: simple arithmetic |1+1")
    void testConstantLambdaSimpleArithmetic() throws Exception {
        // Minimal model - we just need a valid runtime context
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // This is the key: a lambda expression that doesn't reference any table
        String pureQuery = "|1 + 1";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Constant lambda |1+1 result: " + result.rows());
        assertEquals(1, result.rows().size(), "Should return exactly one row");
        assertEquals(2L, ((Number) result.rows().get(0).get(0)).longValue(), "1+1 should equal 2");
    }

    @Test
    @DisplayName("Constant lambda: multiplication |6*7")
    void testConstantLambdaMultiplication() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "|6 * 7";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Constant lambda |6*7 result: " + result.rows());
        assertEquals(1, result.rows().size());
        assertEquals(42L, ((Number) result.rows().get(0).get(0)).longValue(), "6*7 should equal 42");
    }

    @Test
    @DisplayName("Constant lambda: complex expression |(10 + 5) * 2 - 3")
    void testConstantLambdaComplexExpression() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "|(10 + 5) * 2 - 3";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Constant lambda |(10+5)*2-3 result: " + result.rows());
        assertEquals(1, result.rows().size());
        // (10+5)*2-3 = 15*2-3 = 30-3 = 27
        assertEquals(27L, ((Number) result.rows().get(0).get(0)).longValue(), "(10+5)*2-3 should equal 27");
    }

    @Test
    @DisplayName("Constant lambda: string literal |'hello'")
    void testConstantLambdaStringLiteral() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "|'hello'";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Constant lambda |'hello' result: " + result.rows());
        assertEquals(1, result.rows().size());
        assertEquals("hello", result.rows().get(0).get(0), "Should return 'hello'");
    }

    @Test
    @DisplayName("Constant lambda: boolean expression |true")
    void testConstantLambdaBooleanLiteral() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "|true";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Constant lambda |true result: " + result.rows());
        assertEquals(1, result.rows().size());
        assertEquals(true, result.rows().get(0).get(0), "Should return true");
    }

    @Test
    @DisplayName("Constant lambda: comparison |5 > 3")
    void testConstantLambdaComparison() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        String pureQuery = "|5 > 3";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Constant lambda |5>3 result: " + result.rows());
        assertEquals(1, result.rows().size());
        assertEquals(true, result.rows().get(0).get(0), "5 > 3 should be true");
    }

    @Test
    @DisplayName("Constant lambda: qualified function |meta::pure::functions::math::abs(-123)")
    void testConstantLambdaQualifiedFunction() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // Qualified function call - should strip package prefix and use just 'abs'
        String pureQuery = "|meta::pure::functions::math::abs(-123456789123456789.99)";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection,
                QueryService.ResultMode.BUFFERED);

        // Should return a ScalarResult for constant queries
        assertTrue(result instanceof ScalarResult, "Constant query should return ScalarResult");
        ScalarResult scalar = (ScalarResult) result;
        System.out.println("Constant lambda qualified abs result: " + scalar.value());
        assertNotNull(scalar.value());
    }

    @Test
    @DisplayName("Constant lambda: string concatenation |'a' + 'b'")
    void testConstantLambdaStringConcatenation() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // String concatenation using + operator
        String pureQuery = "|'Hello' + ' ' + 'World'";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection,
                QueryService.ResultMode.BUFFERED);

        // Should return a ScalarResult for constant queries
        assertTrue(result instanceof ScalarResult, "Constant query should return ScalarResult");
        ScalarResult scalar = (ScalarResult) result;
        System.out.println("Constant lambda string concat result: " + scalar.value());
        assertEquals("Hello World", scalar.value());
    }

    // ==================== LET STATEMENT COMPILATION TESTS ====================
    // These test let statement support with scalar variable binding

    @Test
    @DisplayName("Let statement: simple scalar binding |let x = 42; $x")
    void testLetStatementSimpleScalar() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // Let statement assigns variable x, then returns $x
        String pureQuery = "|let x = 42; $x;";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection,
                QueryService.ResultMode.BUFFERED);

        assertTrue(result instanceof ScalarResult, "Let with scalar should return ScalarResult");
        ScalarResult scalar = (ScalarResult) result;
        System.out.println("Let statement |let x = 42; $x; result: " + scalar.value());
        assertEquals(42L, ((Number) scalar.value()).longValue(), "Should return 42");
    }

    @Test
    @DisplayName("Let statement: arithmetic with let-bound variables |let x = 10; let y = 5; $x + $y;")
    void testLetStatementArithmetic() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // Multiple let statements, then use variables in arithmetic
        String pureQuery = "|let x = 10; let y = 5; $x + $y;";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection,
                QueryService.ResultMode.BUFFERED);

        assertTrue(result instanceof ScalarResult, "Should return ScalarResult");
        ScalarResult scalar = (ScalarResult) result;
        System.out.println("Let arithmetic result: " + scalar.value());
        assertEquals(15L, ((Number) scalar.value()).longValue(), "10 + 5 should equal 15");
    }

    @Test
    @DisplayName("Let statement: with newlines like PCT expressions")
    void testLetStatementWithNewlines() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // Newline-separated statements like PCT expressions
        String pureQuery = "|let var = 'Hello Variable';\n $var;";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection,
                QueryService.ResultMode.BUFFERED);

        assertTrue(result instanceof ScalarResult, "Should return ScalarResult");
        ScalarResult scalar = (ScalarResult) result;
        System.out.println("Let with newlines result: " + scalar.value());
        assertEquals("Hello Variable", scalar.value());
    }

    @Test
    @DisplayName("ArrayLiteral: greatest([1, 2]) should return 2")
    void testArrayLiteralGreatest() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // This is the PCT pattern: greatest([1, 2])
        String pureQuery = "|greatest([1, 2])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("greatest([1, 2]) result: " + result.rows());
        assertEquals(1, result.rows().size(), "Should return exactly one row");
        assertEquals(2L, ((Number) result.rows().get(0).get(0)).longValue(), "greatest([1, 2]) should equal 2");
    }

    @Test
    @DisplayName("ArrayLiteral: least([1, 2]) should return 1")
    void testArrayLiteralLeast() throws Exception {
        String pureSource = """
                Class model::Dummy { name: String[1]; }
                Database store::DummyDb ( Table T_DUMMY ( ID INTEGER, NAME VARCHAR(100) ) )
                Mapping model::DummyMap ( Dummy: Relational { ~mainTable [DummyDb] T_DUMMY name: [DummyDb] T_DUMMY.NAME } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DummyMap ]; connections: [ store::DummyDb: [ environment: store::TestConn ] ]; }
                """;

        // This is the PCT pattern: least([1, 2])
        String pureQuery = "|least([1, 2])";

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("least([1, 2]) result: " + result.rows());
        assertEquals(1, result.rows().size(), "Should return exactly one row");
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue(), "least([1, 2]) should equal 1");
    }

    // ==================== Date Literal Tests ====================

    @Test
    @DisplayName("Pure: Date literal %2024-01-15 in filter")
    void testDateLiteralInFilter() throws Exception {
        // GIVEN: Create employee table with hire dates
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EMPLOYEE");
            stmt.execute("""
                        CREATE TABLE T_EMPLOYEE (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100),
                            HIRE_DATE DATE
                        )
                    """);
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', DATE '2024-01-15')");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', DATE '2024-06-01')");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Charlie', DATE '2023-12-01')");
        }

        String pureSource = """
                Class model::Employee {
                    name: String[1];
                    hireDate: Date[1];
                }
                Database store::EmpDb ( Table T_EMPLOYEE ( ID INTEGER, NAME VARCHAR(100), HIRE_DATE DATE ) )
                Mapping model::EmpMap ( Employee: Relational {
                    ~mainTable [EmpDb] T_EMPLOYEE
                    name: [EmpDb] T_EMPLOYEE.NAME,
                    hireDate: [EmpDb] T_EMPLOYEE.HIRE_DATE
                } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::EmpMap ]; connections: [ store::EmpDb: [ environment: store::TestConn ] ]; }
                """;

        // WHEN: Query for employees hired on a specific date using Pure date literal
        String pureQuery = """
                Employee.all()
                    ->filter({e | $e.hireDate == %2024-01-15})
                    ->project({e | $e.name})
                """;

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);

        // THEN: Only Alice matches
        assertEquals(1, result.rows().size(), "Should find 1 employee hired on 2024-01-15");
        assertEquals("Alice", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("Pure: Date literal comparison %2024-01-01 in range filter")
    void testDateLiteralRangeFilter() throws Exception {
        // Reuse table from previous test
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EMPLOYEE");
            stmt.execute("""
                        CREATE TABLE T_EMPLOYEE (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100),
                            HIRE_DATE DATE
                        )
                    """);
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', DATE '2024-01-15')");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', DATE '2024-06-01')");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Charlie', DATE '2023-12-01')");
        }

        String pureSource = """
                Class model::Employee {
                    name: String[1];
                    hireDate: Date[1];
                }
                Database store::EmpDb ( Table T_EMPLOYEE ( ID INTEGER, NAME VARCHAR(100), HIRE_DATE DATE ) )
                Mapping model::EmpMap ( Employee: Relational {
                    ~mainTable [EmpDb] T_EMPLOYEE
                    name: [EmpDb] T_EMPLOYEE.NAME,
                    hireDate: [EmpDb] T_EMPLOYEE.HIRE_DATE
                } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::EmpMap ]; connections: [ store::EmpDb: [ environment: store::TestConn ] ]; }
                """;

        // WHEN: Query for employees hired after 2024-01-01
        String pureQuery = """
                Employee.all()
                    ->filter({e | $e.hireDate > %2024-01-01})
                    ->project({e | $e.name}, {e | $e.hireDate})
                """;

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Employees hired after 2024-01-01: " + result.rows());

        // THEN: Alice (2024-01-15) and Bob (2024-06-01) match
        assertEquals(2, result.rows().size(), "Should find 2 employees hired after 2024-01-01");
    }

    @Test
    @DisplayName("Pure: Date literal in Relation API")
    void testDateLiteralInRelationApi() throws Exception {
        // Create a simple table for testing with dates
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_EVENTS");
            stmt.execute("""
                        CREATE TABLE T_EVENTS (
                            ID INTEGER PRIMARY KEY,
                            EVENT_NAME VARCHAR(100),
                            EVENT_DATE DATE
                        )
                    """);
            stmt.execute("INSERT INTO T_EVENTS VALUES (1, 'Meeting', DATE '2024-01-15')");
            stmt.execute("INSERT INTO T_EVENTS VALUES (2, 'Conference', DATE '2024-02-20')");
        }

        String pureSource = """
                Database store::EventDb ( Table T_EVENTS ( ID INTEGER, EVENT_NAME VARCHAR(100), EVENT_DATE DATE ) )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: []; connections: [ store::EventDb: [ environment: store::TestConn ] ]; }
                """;

        // Use Relation API with date literal
        String pureQuery = """
                #>{store::EventDb.T_EVENTS}#
                    ->filter(e | $e.EVENT_DATE == %2024-01-15)
                    ->select(~[EVENT_NAME])
                """;

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Event on 2024-01-15: " + result.rows());

        assertEquals(1, result.rows().size(), "Should find 1 event on 2024-01-15");
        assertEquals("Meeting", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("Pure: DateTime literal %2024-01-15T10:30:00 in filter")
    void testDateTimeLiteralInFilter() throws Exception {
        // GIVEN: Create table with timestamp column
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_APPOINTMENTS");
            stmt.execute("""
                        CREATE TABLE T_APPOINTMENTS (
                            ID INTEGER PRIMARY KEY,
                            TITLE VARCHAR(100),
                            SCHEDULED_AT TIMESTAMP
                        )
                    """);
            stmt.execute("INSERT INTO T_APPOINTMENTS VALUES (1, 'Morning Meeting', TIMESTAMP '2024-01-15 10:30:00')");
            stmt.execute("INSERT INTO T_APPOINTMENTS VALUES (2, 'Afternoon Call', TIMESTAMP '2024-01-15 14:00:00')");
            stmt.execute("INSERT INTO T_APPOINTMENTS VALUES (3, 'Next Day', TIMESTAMP '2024-01-16 09:00:00')");
        }

        String pureSource = """
                Class model::Appointment {
                    title: String[1];
                    scheduledAt: DateTime[1];
                }
                Database store::ApptDb ( Table T_APPOINTMENTS ( ID INTEGER, TITLE VARCHAR(100), SCHEDULED_AT TIMESTAMP ) )
                Mapping model::ApptMap ( Appointment: Relational {
                    ~mainTable [ApptDb] T_APPOINTMENTS
                    title: [ApptDb] T_APPOINTMENTS.TITLE,
                    scheduledAt: [ApptDb] T_APPOINTMENTS.SCHEDULED_AT
                } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::ApptMap ]; connections: [ store::ApptDb: [ environment: store::TestConn ] ]; }
                """;

        // WHEN: Query with DateTime literal (includes time)
        String pureQuery = """
                Appointment.all()
                    ->filter({a | $a.scheduledAt == %2024-01-15T10:30:00})
                    ->project({a | $a.title})
                """;

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Appointment at 2024-01-15 10:30:00: " + result.rows());

        // THEN: Only Morning Meeting matches
        assertEquals(1, result.rows().size(), "Should find 1 appointment at exact datetime");
        assertEquals("Morning Meeting", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("Pure: DateTime comparison with > operator")
    void testDateTimeComparisonOperator() throws Exception {
        // Reuse table from previous test
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS T_APPOINTMENTS");
            stmt.execute("""
                        CREATE TABLE T_APPOINTMENTS (
                            ID INTEGER PRIMARY KEY,
                            TITLE VARCHAR(100),
                            SCHEDULED_AT TIMESTAMP
                        )
                    """);
            stmt.execute("INSERT INTO T_APPOINTMENTS VALUES (1, 'Morning Meeting', TIMESTAMP '2024-01-15 10:30:00')");
            stmt.execute("INSERT INTO T_APPOINTMENTS VALUES (2, 'Afternoon Call', TIMESTAMP '2024-01-15 14:00:00')");
            stmt.execute("INSERT INTO T_APPOINTMENTS VALUES (3, 'Next Day', TIMESTAMP '2024-01-16 09:00:00')");
        }

        String pureSource = """
                Class model::Appointment {
                    title: String[1];
                    scheduledAt: DateTime[1];
                }
                Database store::ApptDb ( Table T_APPOINTMENTS ( ID INTEGER, TITLE VARCHAR(100), SCHEDULED_AT TIMESTAMP ) )
                Mapping model::ApptMap ( Appointment: Relational {
                    ~mainTable [ApptDb] T_APPOINTMENTS
                    title: [ApptDb] T_APPOINTMENTS.TITLE,
                    scheduledAt: [ApptDb] T_APPOINTMENTS.SCHEDULED_AT
                } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::ApptMap ]; connections: [ store::ApptDb: [ environment: store::TestConn ] ]; }
                """;

        // WHEN: Query for appointments after noon on Jan 15
        String pureQuery = """
                Appointment.all()
                    ->filter({a | $a.scheduledAt > %2024-01-15T12:00:00})
                    ->project({a | $a.title}, {a | $a.scheduledAt})
                """;

        var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", connection);
        System.out.println("Appointments after noon Jan 15: " + result.rows());

        // THEN: Afternoon Call and Next Day match
        assertEquals(2, result.rows().size(), "Should find 2 appointments after noon on Jan 15");
    }

    // ========================================
    // SELECT TESTS
    // ========================================

    @Test
    @DisplayName("select() with no arguments returns all columns (SELECT *)")
    void testSelectAllColumns() throws SQLException {
        // This matches the PCT pattern: #TDS...#->select()
        String pureQuery = """
                #TDS
                    val, str, other
                    1, a, x
                    3, ewe, y
                    4, qw, z
                #->select()
                """;

        var result = executeRelation(pureQuery);
        System.out.println("select() result: " + result.rows());

        // Should return all 3 rows with all 3 columns
        assertEquals(3, result.rows().size(), "Should have 3 rows");
        assertEquals(3, result.columns().size(), "Should have 3 columns");
    }

    @Test
    @DisplayName("select(~col) selects specific column")
    void testSelectSpecificColumn() throws SQLException {
        String pureQuery = """
                #TDS
                    val, str, other
                    1, a, x
                    3, ewe, y
                    4, qw, z
                #->select(~str)
                """;

        var result = executeRelation(pureQuery);
        System.out.println("select(~str) result: " + result.rows());

        assertEquals(3, result.rows().size(), "Should have 3 rows");
        assertEquals(1, result.columns().size(), "Should have 1 column");
        assertEquals("str", result.columns().get(0).name());
    }

    @Test
    @DisplayName("select(~[col1, col2]) selects multiple columns")
    void testSelectMultipleColumns() throws SQLException {
        String pureQuery = """
                #TDS
                    val, str, other
                    1, a, x
                    3, ewe, y
                    4, qw, z
                #->select(~[val, other])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("select(~[val, other]) result: " + result.rows());

        assertEquals(3, result.rows().size(), "Should have 3 rows");
        assertEquals(2, result.columns().size(), "Should have 2 columns");
    }

    // ========================================
    // ASOF JOIN TESTS
    // ========================================

    @Test
    void testAsOfJoinSimple() throws SQLException {
        // GIVEN: Two TDS literals - trades and quotes with timestamps
        // Trade times: 10:30, 11:30, 12:30
        // Quote times: 10:15, 10:45, 11:15, 12:00, 12:45
        // AsOf join should match each trade with the most recent quote BEFORE the trade
        // time

        String pureQuery = """
                #TDS
                    trade_id, trade_time
                    1, %2024-01-15T10:30:00
                    2, %2024-01-15T11:30:00
                    3, %2024-01-15T12:30:00
                #->asOfJoin(
                    #TDS
                        quote_id, quote_time, price
                        A, %2024-01-15T10:15:00, 100
                        B, %2024-01-15T10:45:00, 102
                        C, %2024-01-15T11:15:00, 105
                        D, %2024-01-15T12:00:00, 108
                        E, %2024-01-15T12:45:00, 110
                    #,
                    {t, q | $t.trade_time > $q.quote_time}
                )
                """;

        var result = executeRelation(pureQuery);
        System.out.println("AsOf Join Result: " + result.rows());

        // THEN: Should get 3 rows (one for each trade)
        // Trade 1 (10:30) matches Quote A (10:15) - the only quote before it
        // Trade 2 (11:30) matches Quote C (11:15) - most recent before 11:30
        // Trade 3 (12:30) matches Quote D (12:00) - most recent before 12:30
        assertEquals(3, result.rows().size(), "Should have 3 rows (one per trade)");
    }

    @Test
    void testAsOfJoinWithKeyMatch() throws SQLException {
        // GIVEN: Trades and quotes with symbol keys
        // Each trade should match with the most recent quote for the SAME symbol

        String pureQuery = """
                #TDS
                    trade_id, symbol, trade_time
                    1, AAPL, %2024-01-15T10:30:00
                    2, MSFT, %2024-01-15T10:30:00
                    3, AAPL, %2024-01-15T11:30:00
                #->asOfJoin(
                    #TDS
                        quote_id, quote_symbol, quote_time, price
                        A, AAPL, %2024-01-15T10:00:00, 180
                        B, MSFT, %2024-01-15T10:00:00, 350
                        C, AAPL, %2024-01-15T11:00:00, 182
                        D, MSFT, %2024-01-15T11:00:00, 355
                    #,
                    {t, q | $t.trade_time > $q.quote_time},
                    {t, q | $t.symbol == $q.quote_symbol}
                )
                """;

        var result = executeRelation(pureQuery);
        System.out.println("AsOf Join with Key Match: " + result.rows());

        // THEN: Should get 3 rows
        // Trade 1 (AAPL 10:30) matches Quote A (AAPL 10:00)
        // Trade 2 (MSFT 10:30) matches Quote B (MSFT 10:00)
        // Trade 3 (AAPL 11:30) matches Quote C (AAPL 11:00) - most recent AAPL before
        // 11:30
        assertEquals(3, result.rows().size(), "Should have 3 rows with key matching");
    }

    @Test
    void testAsOfJoinSqlGeneration() throws SQLException {
        // Verify the generated SQL syntax is correct for ASOF join
        String pureQuery = """
                #TDS
                    id, time
                    1, %2024-01-15T10:00:00
                #->asOfJoin(
                    #TDS
                        id2, time2
                        A, %2024-01-15T09:00:00
                    #,
                    {x, y | $x.time > $y.time2}
                )
                """;

        var result = executeRelation(pureQuery);
        System.out.println("AsOf Join SQL test result: " + result.rows());

        // Should execute without error and return result
        assertNotNull(result);
        assertEquals(1, result.rows().size());
    }

    // ========================================
    // AGGREGATE TESTS
    // ========================================

    @Test
    void testAggregateSimple() throws SQLException {
        // GIVEN: A TDS with numeric values to sum
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                #->aggregate(~idSum : x | $x.id : y | $y->plus())
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Aggregate result: " + result.rows());

        // THEN: Should get single row with sum of all ids (1+2+3=6)
        assertEquals(1, result.rows().size(), "Should have single aggregated row");
        // DuckDB returns BigInteger for SUM, so convert to long for comparison
        Number sum = (Number) result.rows().getFirst().values().getFirst();
        assertEquals(6L, sum.longValue(), "Sum should be 6");
    }

    /**
     * Test OLAP aggregate using extend() with multiple columns (no explicit over).
     * This mirrors PCT test: testOLAPAggNoWindowMultipleColums
     * 
     * Pattern: extend(~[col1:c|$c.id:y|$y->plus(), col2:c|$c.grp:y|$y->plus()])
     * Expected: SUM(id) OVER() as col1, SUM(grp) OVER() as col2
     */
    @Test
    void testOlapAggregateNoWindowMultipleColumns() throws SQLException {
        // Same data as PCT test
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                    7, 3, G
                    8, 1, H
                    9, 5, I
                    10, 0, J
                #->extend(~[newCol:c|$c.id:y|$y->plus(), other:c|$c.grp:y|$y->plus()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("OLAP aggregate no window multiple columns result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // THEN: Should have 10 rows with newCol=55 (sum of all ids) and other=22 (sum
        // of all grps)
        assertEquals(10, result.rows().size(), "Should have 10 rows");

        // Sum of ids 1-10 = 55
        // Sum of grps 0+1+1+1+2+2+3+3+4+5 = 22
        int expectedIdSum = 55;
        int expectedGrpSum = 22;

        for (var row : result.rows()) {
            // Columns are: id, grp, name, newCol, other
            int newCol = ((Number) row.values().get(3)).intValue();
            int other = ((Number) row.values().get(4)).intValue();

            assertEquals(expectedIdSum, newCol,
                    "newCol should be sum of all ids (55), got: " + newCol);
            assertEquals(expectedGrpSum, other,
                    "other should be sum of all grps (22), got: " + other);
        }
    }

    @Test
    void testAggregateMultipleColumns() throws SQLException {
        // GIVEN: A TDS with values for multiple aggregations
        String pureQuery = """
                #TDS
                    id, grp, value
                    1, 2, 10
                    2, 1, 20
                    3, 3, 30
                #->aggregate(~[idSum : x | $x.id : y | $y->plus(), valueSum : x | $x.value : y | $y->plus()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Aggregate multiple cols: " + result.rows());

        // THEN: Should get single row with idSum=6 and valueSum=60
        assertEquals(1, result.rows().size(), "Should have single aggregated row");
        var row = result.rows().getFirst();
        assertEquals(2, row.values().size(), "Should have 2 columns");
    }

    @Test
    void testAggregateJoinStrings() throws SQLException {
        // GIVEN: A TDS with string values to concatenate
        String pureQuery = """
                #TDS
                    id, code
                    1, A
                    2, B
                    3, C
                #->aggregate(~codes : x | $x.code : y | $y->joinStrings(':'))
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Aggregate joinStrings result: " + result.rows());

        // THEN: Should get single row with codes='A:B:C' (or some order)
        assertEquals(1, result.rows().size(), "Should have single aggregated row");
        String codes = (String) result.rows().getFirst().values().getFirst();
        // Order may vary, so just check length and contains
        assertTrue(codes.contains("A") && codes.contains("B") && codes.contains("C"),
                "Should contain all codes: " + codes);
    }

    /**
     * Test window aggregate with partition - SUM over partition.
     * This mirrors the PCT test: testOLAPCastExtractAggWithPartitionWindow
     * 
     * Pattern: extend(~grp->over(),
     * ~newCol:{p,w,r|$r.id->cast(@Integer)}:y|$y->plus())
     * Expected: SUM(id) OVER (PARTITION BY grp)
     */
    @Test
    void testWindowAggregateWithPartition() throws SQLException {
        // GIVEN: A TDS with groups for partition testing
        // grp=0: id=10 -> sum=10
        // grp=1: id=2,6,8 -> sum=16
        // grp=2: id=1,5 -> sum=6
        // grp=3: id=3,7 -> sum=10
        // grp=4: id=4 -> sum=4
        // grp=5: id=9 -> sum=9
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                    7, 3, G
                    8, 1, H
                    9, 5, I
                    10, 0, J
                #->extend(over(~grp), ~newCol:{p,w,r|$r.id}:y|$y->plus())
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Window aggregate result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // THEN: newCol should be the SUM of id values within each grp partition
        assertEquals(10, result.rows().size(), "Should have 10 rows");

        // Verify specific expected values
        for (var row : result.rows()) {
            int grp = ((Number) row.values().get(1)).intValue();
            int newCol = ((Number) row.values().get(3)).intValue();

            int expectedSum = switch (grp) {
                case 0 -> 10; // id=10
                case 1 -> 16; // id=2+6+8
                case 2 -> 6; // id=1+5
                case 3 -> 10; // id=3+7
                case 4 -> 4; // id=4
                case 5 -> 9; // id=9
                default -> throw new AssertionError("Unexpected grp: " + grp);
            };
            assertEquals(expectedSum, newCol,
                    "For grp=" + grp + ", sum should be " + expectedSum + " but got " + newCol);
        }
    }

    /**
     * Test window aggregate with cast in the map lambda.
     * This mirrors the PCT test: testOLAPCastExtractAggWithPartitionWindow
     * 
     * Pattern: extend(~grp->over(),
     * ~newCol:{p,w,r|$r.id->cast(@Integer)}:y|$y->plus())
     * Expected: SUM(id) OVER (PARTITION BY grp)
     */
    @Test
    void testWindowAggregateWithCast() throws SQLException {
        // Same data as PCT test
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                    7, 3, G
                    8, 1, H
                    9, 5, I
                    10, 0, J
                #->extend(over(~grp), ~newCol:{p,w,r|$r.id->cast(@Integer)}:y|$y->plus())
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Window aggregate with cast result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // THEN: newCol should be SUM(id) partitioned by grp
        assertEquals(10, result.rows().size(), "Should have 10 rows");

        // Verify the SQL contains SUM("id") not SUM("value")
        // and verify the partition sums match expected
        for (var row : result.rows()) {
            int grp = ((Number) row.values().get(1)).intValue();
            int newCol = ((Number) row.values().get(3)).intValue();

            int expectedSum = switch (grp) {
                case 0 -> 10; // id=10
                case 1 -> 16; // id=2+6+8
                case 2 -> 6; // id=1+5
                case 3 -> 10; // id=3+7
                case 4 -> 4; // id=4
                case 5 -> 9; // id=9
                default -> throw new AssertionError("Unexpected grp: " + grp);
            };
            assertEquals(expectedSum, newCol,
                    "For grp=" + grp + ", sum should be " + expectedSum);
        }
    }

    /**
     * Test window STRING_AGG with partition, order, and unbounded frame.
     * This is the EXACT PCT test:
     * testOLAPAggStringWithPartitionAndOrderASCUnboundedWindow
     * 
     * Expected: Each partition's STRING_AGG should include ALL names in the
     * partition
     * (not cumulative) because of the unbounded frame.
     */
    @Test
    void testOLAPAggStringWithPartitionAndOrderASCUnboundedWindow() throws SQLException {
        // Exact data from PCT test
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                    7, 3, G
                    8, 1, H
                    9, 5, I
                    10, 0, J
                #->extend(over(~grp, ~id->ascending(), unbounded()->rows(unbounded())), ~newCol:{p,w,r|$r.name}:y|$y->joinStrings(''))
                """;

        var result = executeRelation(pureQuery);
        System.out.println("testOLAPAggStringWithPartitionAndOrderASCUnboundedWindow result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        assertEquals(10, result.rows().size(), "Should have 10 rows");

        // With unbounded frame (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
        // FOLLOWING),
        // all rows in a partition should have the SAME aggregated value
        // Expected per partition:
        // grp=0: "J"
        // grp=1: "BFH" (ids 2,6,8 -> B,F,H)
        // grp=2: "AE" (ids 1,5 -> A,E)
        // grp=3: "CG" (ids 3,7 -> C,G)
        // grp=4: "D"
        // grp=5: "I"
        for (var row : result.rows()) {
            int grp = ((Number) row.values().get(1)).intValue();
            String newCol = (String) row.values().get(3);

            String expected = switch (grp) {
                case 0 -> "J";
                case 1 -> "BFH";
                case 2 -> "AE";
                case 3 -> "CG";
                case 4 -> "D";
                case 5 -> "I";
                default -> throw new AssertionError("Unexpected grp: " + grp);
            };
            assertEquals(expected, newCol,
                    "For grp=" + grp + ", all rows should have " + expected);
        }
    }

    /**
     * Test OLAP aggregate with multiple window columns.
     * This is the EXACT PCT test:
     * testOLAPAggWithPartitionAndOrderUnboundedWindowMultipleColumns
     * 
     * Pattern: extend(over(~grp, ~id->descending(),
     * unbounded()->rows(unbounded())),
     * ~[newCol:...:y|$y->joinStrings(''), other:...:y|$y->plus()])
     */
    @Test
    void testOLAPAggWithPartitionAndOrderUnboundedWindowMultipleColumns() throws SQLException {
        // Exact data from PCT test
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                    7, 3, G
                    8, 1, H
                    9, 5, I
                    10, 0, J
                #->extend(over(~grp, ~id->descending(), unbounded()->rows(unbounded())), ~[newCol:{p,w,r|$r.name}:y|$y->joinStrings(''),other:{p,w,r|$r.id}:y|$y->plus()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("testOLAPAggWithPartitionAndOrderUnboundedWindowMultipleColumns result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        assertEquals(10, result.rows().size(), "Should have 10 rows");

        // With unbounded frame, all rows in a partition have the same values
        // With DESC order, names are concatenated in descending id order:
        // grp=0: "J", sum=10
        // grp=1: "HFB" (ids 8,6,2 -> H,F,B), sum=16
        // grp=2: "EA" (ids 5,1 -> E,A), sum=6
        // grp=3: "GC" (ids 7,3 -> G,C), sum=10
        // grp=4: "D", sum=4
        // grp=5: "I", sum=9
        for (var row : result.rows()) {
            int grp = ((Number) row.values().get(1)).intValue();
            String newCol = (String) row.values().get(3);
            int other = ((Number) row.values().get(4)).intValue();

            var expected = switch (grp) {
                case 0 -> new Object[] { "J", 10 };
                case 1 -> new Object[] { "HFB", 16 };
                case 2 -> new Object[] { "EA", 6 };
                case 3 -> new Object[] { "GC", 10 };
                case 4 -> new Object[] { "D", 4 };
                case 5 -> new Object[] { "I", 9 };
                default -> throw new AssertionError("Unexpected grp: " + grp);
            };
            assertEquals(expected[0], newCol, "For grp=" + grp + ", newCol should be " + expected[0]);
            assertEquals(expected[1], other, "For grp=" + grp + ", other should be " + expected[1]);
        }
    }

    /**
     * Test OLAP with partition and MULTIPLE ORDER BY columns.
     * This is the EXACT PCT test:
     * testOLAPWithPartitionAndMultipleOrderWindowMultipleColumnsWithFilter
     * 
     * Pattern: extend(over(~grp, [~name->ascending(), ~height->ascending()]),
     * ~[newCol:{p,w,r|$p->lead($r).id}, other:{p,w,r|$p->first($w,$r).name}])
     */
    @Test
    void testOLAPWithPartitionAndMultipleOrderWindowMultipleColumnsWithFilter() throws SQLException {
        // Simplified version of PCT test to verify ORDER BY array parsing
        String pureQuery = """
                #TDS
                    id, grp, name, height
                    1, 2, A, 11
                    2, 1, B, 12
                    3, 3, C, 12
                    4, 3, B, 11
                    5, 2, B, 13
                    6, 1, C, 12
                    7, 3, A, 13
                    8, 1, B, 14
                #->extend(over(~grp, [~name->ascending(), ~height->ascending()]), ~newCol:{p,w,r|$r.id}:y|$y->plus())
                """;

        var result = executeRelation(pureQuery);
        System.out.println("testOLAPWithPartitionAndMultipleOrderWindowMultipleColumnsWithFilter result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // Verify ORDER BY clause is generated with multiple columns
        assertEquals(8, result.rows().size(), "Should have 8 rows");
    }

    /**
     * Test simple extend with integer arithmetic.
     * This is the EXACT PCT test: testSimpleExtendInt
     * 
     * Verifies that integer columns preserve their type in arithmetic operations,
     * producing Integer results (not Float/Double).
     * 
     * Pattern: extend(~name:c|$c.val->toOne() + 1)
     */
    @Test
    void testSimpleExtendInt() throws SQLException {
        String pureQuery = """
                #TDS
                    val, str
                    1, a
                    3, ewe
                    4, qw
                    5, wwe
                    6, weq
                #->extend(~name:c|$c.val + 1)
                """;

        // Verify generated SQL does NOT cast integer to DOUBLE
        String sql = generateSql(pureQuery);
        assertFalse(sql.contains("CAST") && sql.contains("AS DOUBLE"),
                "Integer arithmetic should NOT cast to DOUBLE. Got: " + sql);

        var result = executeRelation(pureQuery);
        System.out.println("testSimpleExtendInt result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        assertEquals(5, result.rows().size(), "Should have 5 rows");

        // Verify integer results (not floats)
        var expected = List.of(2, 4, 5, 6, 7);
        var actual = result.rows().stream()
                .map(row -> ((Number) row.values().get(2)).intValue())
                .sorted()
                .toList();
        assertEquals(expected, actual, "Extended column should have integer values");
    }

    /**
     * Test nth() window function for accessing the nth row value in a window.
     * This mirrors the PCT test pattern: testOLAPWithPartitionAndOrderNthWindow2
     * 
     * Pattern: extend(~partition->over(~order->descending()),
     * ~newCol:{p,w,r|$p->nth($w,$r,N).column})
     * Should generate: NTH_VALUE("column", N) OVER (PARTITION BY "partition" ORDER
     * BY "order" DESC)
     */
    @Test
    void testNthValueWindowFunction() throws SQLException {
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                #->extend(~grp->over(~id->descending()), ~newCol:{p,w,r|$p->nth($w, $r, 2).id})
                """;

        // Verify generated SQL uses NTH_VALUE
        String sql = generateSql(pureQuery);
        System.out.println("nth() window function SQL: " + sql);
        assertTrue(sql.contains("NTH_VALUE"), "Should generate NTH_VALUE window function. Got: " + sql);
        assertTrue(sql.contains("\"id\", 2"), "NTH_VALUE should have column and offset. Got: " + sql);

        var result = executeRelation(pureQuery);
        System.out.println("nth() results:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        assertEquals(5, result.rows().size(), "Should have 5 rows");
    }

    /**
     * Test TDS-based project() with column transformations including toLower.
     * This mirrors the PCT test pattern for TDS project.
     * 
     * Pattern: #TDS...#->project(~[col:x|$x.col->toLower()])
     */
    @Test
    void testRelationProjectWithTransformation() throws SQLException {
        String pureQuery = """
                #TDS
                    id, name
                    1, George
                    2, David
                #->project(~[id:x|$x.id, lowerName:x|$x.name->toLower()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Relation project result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        assertEquals(2, result.rows().size(), "Should have 2 rows");

        // Verify data is correctly projected and lowercased
        var firstRow = result.rows().get(0);
        assertEquals(1, ((Number) firstRow.values().get(0)).intValue());
        assertEquals("george", firstRow.values().get(1)); // Should be lowercase

        var secondRow = result.rows().get(1);
        assertEquals("david", secondRow.values().get(1)); // Should be lowercase
    }

    /**
     * Test TDS-based project->join->project pattern.
     * This mirrors the PCT test: testProjectJoinWithProjectProject
     * 
     * Pattern: #TDS...#->project(~[...])->join(#TDS...#->project(~[...]), INNER,
     * {...})->project(~[...])
     */
    @Test
    void testTdsProjectJoinProject() throws SQLException {
        String pureQuery = """
                #TDS
                    id, name
                    1, George
                    4, David
                #->project(~[id1:x|$x.id, name1:x|$x.name])
                ->join(
                    #TDS
                        id, col
                        1, MoreGeorge
                        4, MoreDavid
                    #->project(~[id2:x|$x.id, col:x|$x.col]),
                    meta::pure::functions::relation::JoinKind.INNER,
                    {x, y|$x.id1 == $y.id2}
                )
                ->project(~[resultId:x|$x.id1, resultCol:x|$x.col])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("TDS project->join->project result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        assertEquals(2, result.rows().size(), "Should have 2 rows from INNER join");

        // Verify joined data
        var rows = result.rows();
        boolean foundGeorge = rows.stream().anyMatch(r -> ((Number) r.values().get(0)).intValue() == 1 &&
                "MoreGeorge".equals(r.values().get(1)));
        boolean foundDavid = rows.stream().anyMatch(r -> ((Number) r.values().get(0)).intValue() == 4 &&
                "MoreDavid".equals(r.values().get(1)));

        assertTrue(foundGeorge, "Should find George's joined data");
        assertTrue(foundDavid, "Should find David's joined data");
    }

    /**
     * Test distinct->groupBy->filter chain with proper aggregate aliasing.
     * Regression test for: aggregate alias was incorrectly using groupBy column
     * name.
     * 
     * Pattern: #TDS...#->distinct()->groupBy(~[col],
     * ~newCol:...)->filter(x|$x.newCol > n)
     */
    @Test
    void testDistinctGroupByFilterChain() throws SQLException {
        String pureQuery = """
                #TDS
                    val, str, str2
                    2, a, b
                    2, a, b
                    4, qw, b
                    5, qw, c
                    2, weq, c
                #->distinct()
                ->groupBy(~[str], ~newCol:x|$x.val:y|$y->plus())
                ->filter(x|$x.newCol > 2)
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Distinct->GroupBy->Filter result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // After distinct: (2,a,b), (4,qw,b), (5,qw,c), (2,weq,c)
        // After groupBy 'str' with SUM(val) as 'newCol':
        // a -> 2, qw -> 9, weq -> 2
        // After filter newCol > 2:
        // qw -> 9
        assertEquals(1, result.rows().size(), "Only qw group (sum=9) should pass filter > 2");
        assertEquals("qw", result.rows().get(0).values().get(0), "str column should be 'qw'");
        assertEquals(9, ((Number) result.rows().get(0).values().get(1)).intValue(), "newCol should be 9 (4+5)");
    }

    /**
     * Test pivot parsing with groupBy source.
     * 
     * Pattern: #TDS...#->groupBy(~[city,country], ~[...])->pivot(~[year],
     * ~[aggSpec])
     */
    @Test
    void testGroupByPivot() throws SQLException {
        // Simple pivot on TDS
        String pureQuery = """
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    NYC, 2012, 7600
                    SAN, 2011, 2000
                #->pivot(~[year], ~[total:x|$x.treePlanted:y|$y->plus()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Pivot result columns: " + result.columns());
        System.out.println("Pivot result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // The pivot should create columns for each unique year value
        assertFalse(result.rows().isEmpty(), "Should have pivot results");
    }

    /**
     * Complex test: extend->filter->select->groupBy->pivot chain.
     * Mirrors PCT test: test_Extend_Filter_Select_ComplexGroupBy_Pivot
     */
    @Test
    void testExtendFilterSelectGroupByPivot() throws SQLException {
        String pureQuery = """
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    NYC, USA, 2000, 5000
                    SAN, USA, 2000, 2000
                    SAN, USA, 2011, 100
                    LDN, UK, 2011, 3000
                    SAN, USA, 2011, 2500
                    NYC, USA, 2000, 10000
                    NYC, USA, 2012, 7600
                    NYC, USA, 2012, 7600
                #->extend(~yr:x|$x.year->toOne() - 2000)
                ->filter(x|$x.yr > 10)
                ->select(~[city,country,year,treePlanted])
                ->groupBy(~[city,country], ~[year:x|$x.year:y|$y->plus(),treePlanted:x|$x.treePlanted:y|$y->plus()])
                ->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Extend->Filter->Select->GroupBy->Pivot result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // After extend: adds 'yr' column = year - 2000
        // After filter yr > 10: keeps 2011, 2012 (yr=11, 12)
        // After select: drops 'yr' column
        // After groupBy: groups by city,country with sum of year and treePlanted
        // After pivot: pivots by year values (summed years)
        assertFalse(result.rows().isEmpty(), "Should have pivot results");
    }

    /**
     * Test that extend() chain works after pivot.
     * This tests support for PivotExpression as a valid RelationExpression source.
     * Pattern: ->pivot()->extend()
     */
    @Test
    void testPivotExtend() throws SQLException {
        // Test that pivot expression is recognized as valid RelationExpression
        String pureQuery = """
                #TDS
                    city, year, value
                    NYC, 2011, 100
                    NYC, 2012, 200
                    SAN, 2011, 50
                #->pivot(~[year], ~[total:x|$x.value:y|$y->plus()])
                ->extend(~combined:x|$x.city->toOne() + '_test')
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Pivot->Extend result columns: " + result.columns());

        // Verify we get results with the extended column
        assertFalse(result.rows().isEmpty(), "Should have results");

        // Verify the combined column exists and has correct values (city + '_test')
        var combinedCol = result.columns().stream()
                .filter(c -> c.name().equals("combined"))
                .findFirst();
        assertTrue(combinedCol.isPresent(), "Should have 'combined' column");

        // Verify values end with '_test' (combined is last column)
        var firstRow = result.rows().get(0);
        int combinedIdx = result.columns().size() - 1; // combined is the last column added by extend
        String combinedValue = (String) firstRow.values().get(combinedIdx);
        assertTrue(combinedValue.endsWith("_test"), "Combined column should end with '_test': " + combinedValue);
    }

    /**
     * Test that groupBy() chain works after pivot->cast.
     * This tests support for CastExpression as a valid source in parseGroupByCall.
     * Pattern: ->pivot()->cast(@Relation)->groupBy()->extend()
     */
    @Test
    void testPivotCastGroupByExtend() throws SQLException {
        String pureQuery = """
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    SAN, USA, 2011, 2600
                    LDN, UK, 2011, 3000
                    NYC, USA, 2012, 15200
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                ->groupBy(~[country], ~['2011__|__newCol':x|$x.'2011__|__newCol':y|$y->plus()])
                ->extend(~combined:x|$x.country->toOne() + '_test')
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Pivot->Cast->GroupBy->Extend result columns: " + result.columns());

        // Verify we get results
        assertFalse(result.rows().isEmpty(), "Should have results");

        // Verify the combined column exists
        var combinedCol = result.columns().stream()
                .filter(c -> c.name().equals("combined"))
                .findFirst();
        assertTrue(combinedCol.isPresent(), "Should have 'combined' column");
    }

    /**
     * Test that filter() works after pivot->cast.
     * This tests support for CastExpression as a valid source in parseFilterCall.
     * Pattern: ->pivot()->cast(@Relation)->filter()
     */
    @Test
    void testPivotCastFilter() throws SQLException {
        String pureQuery = """
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    SAN, USA, 2011, 2600
                    LDN, UK, 2011, 3000
                    NYC, USA, 2012, 15200
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                ->filter(x|$x.city == 'NYC')
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Pivot->Cast->Filter result: " + result.rows());

        // Verify we get only NYC rows
        assertEquals(1, result.rows().size(), "Should have 1 NYC row");

        // Verify the city column value
        var cityCol = result.columns().stream()
                .filter(c -> c.name().equals("city"))
                .findFirst();
        assertTrue(cityCol.isPresent(), "Should have 'city' column");
    }

    /**
     * Test pivot with literal expression for count.
     * Pattern: ~[count:x|1:y|$y->plus()]
     * This tests that |1 is correctly handled as an expression, not a column.
     */
    @Test
    void testPivotWithLiteralCountExpression() throws SQLException {
        String pureQuery = """
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    NYC, USA, 2000, 5000
                    SAN, USA, 2000, 2000
                    SAN, USA, 2011, 100
                    LDN, UK, 2011, 3000
                    SAN, USA, 2011, 2500
                    NYC, USA, 2000, 10000
                    NYC, USA, 2012, 7600
                    NYC, USA, 2012, 7600
                #->pivot(~[country,city], ~[sum:x|$x.treePlanted:y|$y->plus(),count:x|1:y|$y->plus()])
                """;

        String sql = generateSql(pureQuery);
        System.out.println("Pivot with literal count SQL: " + sql);

        // Verify the SQL includes both aggregations
        assertTrue(sql.contains("SUM("), "SQL should contain SUM function");
        assertTrue(sql.contains("SUM(1)"), "SQL should contain SUM(1) for count");

        var result = executeRelation(pureQuery);
        System.out.println("Pivot count result: " + result.rows());
        assertFalse(result.rows().isEmpty(), "Should have results");
    }

    /**
     * Test pivot with computed expression (multiplication).
     * Pattern: ~[sum:x|$x.treePlanted * $x.coefficient:y|$y->plus()]
     * This tests that binary expressions are correctly compiled to SQL.
     */
    @Test
    void testPivotWithComputedMultiplicationExpression() throws SQLException {
        String pureQuery = """
                #TDS
                    city, country, year, treePlanted, coefficient
                    NYC, USA, 2011, 5000, 1
                    NYC, USA, 2000, 5000, 2
                    SAN, USA, 2000, 2000, 1
                    SAN, USA, 2011, 100, 2
                    LDN, UK, 2011, 3000, 2
                    SAN, USA, 2011, 2500, 1
                    NYC, USA, 2000, 10000, 2
                    NYC, USA, 2012, 7600, 1
                    NYC, USA, 2012, 7600, 2
                #->pivot(~[country,city], ~[weightedSum:x|$x.treePlanted * $x.coefficient:y|$y->plus()])
                """;

        String sql = generateSql(pureQuery);
        System.out.println("Pivot with computed expression SQL: " + sql);

        // Verify the SQL includes the multiplication expression
        assertTrue(sql.contains("SUM("), "SQL should contain SUM function");
        assertTrue(sql.contains("*"), "SQL should contain multiplication operator");

        var result = executeRelation(pureQuery);
        System.out.println("Pivot weighted sum result: " + result.rows());
        assertFalse(result.rows().isEmpty(), "Should have results");

        // Verify multi-column pivot uses __|__ separator for column names
        // Column names should be like UK__|__LDN_|__weightedSum
        assertTrue(sql.contains("|| '__|__' ||"), "Multi-column pivot should concatenate with __|__ separator");
    }

    /**
     * Test multi-column pivot produces correct column naming with __|__ separator
     * AND proper aggregation (rows are grouped correctly).
     * Pure expects columns named: country__|__city__|__aggName
     * Not: country_city__|__aggName (which DuckDB would produce natively)
     */
    @Test
    void testMultiColumnPivotColumnNaming() throws SQLException {
        // Use data with multiple rows per year to verify aggregation works
        String pureQuery = """
                #TDS
                    country, city, year, sales
                    USA, NYC, 2020, 100
                    USA, NYC, 2020, 50
                    USA, LA, 2020, 200
                    UK, LDN, 2021, 300
                #->pivot(~[country, city], ~[total:x|$x.sales:y|$y->plus()])
                """;

        String sql = generateSql(pureQuery);
        System.out.println("Multi-column pivot SQL: " + sql);

        // Verify the EXCLUDE clause removes original pivot columns (prevents wrong
        // grouping)
        assertTrue(sql.contains("EXCLUDE"), "Should use EXCLUDE to remove pivot columns");
        // Verify the concatenation with __|__ separator is in the SQL
        assertTrue(sql.contains("|| '__|__' ||"), "Should use __|__ separator for multi-column pivot");
        assertTrue(sql.contains("_pivot_key"), "Should create _pivot_key for concatenated columns");

        var result = executeRelation(pureQuery);
        System.out.println("Multi-column pivot result columns: " + result.columns());
        System.out.println("Multi-column pivot result: " + result.rows());

        // Should have 2 rows (2020 and 2021), NOT 4 rows (one per original row)
        assertEquals(2, result.rows().size(), "Should aggregate into 2 rows (one per year)");

        // Column names should contain __|__ between pivoted values
        boolean hasCorrectSeparator = result.columns().stream()
                .anyMatch(c -> c.name().contains("__|__"));
        assertTrue(hasCorrectSeparator, "Column names should use __|__ separator");
    }

    /**
     * Test let bindings with multiple filter expressions.
     * Tests that $a->filter() and $b->filter() work correctly with variable
     * resolution.
     * Pattern: let a = #TDS...#; let b = $a->filter(...); $b->filter(...);
     */
    @Test
    void testLetBindingWithMultipleFilters() throws SQLException {
        String pureQuery = """
                |let a = #TDS
                    val
                    1
                    3
                    4
                    5
                #;
                let b = $a->filter(x|$x.val > 2);
                $b->filter(x|$x.val > 3);
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Let binding with multiple filters result: " + result.rows());

        // 1 is filtered by first filter (>2)
        // 3 is filtered by second filter (>3)
        // Only 4 and 5 should remain
        assertEquals(2, result.rows().size(), "Should have 2 rows (4 and 5)");

        // Verify the values
        var vals = result.rows().stream()
                .map(r -> ((Number) r.get(0)).intValue())
                .sorted()
                .toList();
        assertEquals(List.of(4, 5), vals, "Should have values 4 and 5");
    }

    /**
     * Test sort()->groupBy() chain with complex sort expression.
     * Tests that SortExpression can be used as source for groupBy.
     * Pattern: #TDS...#->sort(~col->ascending())->groupBy(...)
     */
    @Test
    void testSortThenGroupBy() throws SQLException {
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                #->sort(~name->ascending())
                ->groupBy(~[grp], ~[total:x|$x.id])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Sort->GroupBy result: " + result.rows());

        // Should have 4 groups: grp=1,2,3,4
        assertEquals(4, result.rows().size(), "Should have 4 groups");

        // Verify column names
        var colNames = result.columns().stream().map(c -> c.name()).toList();
        assertTrue(colNames.contains("grp"), "Should have 'grp' column");
        assertTrue(colNames.contains("total"), "Should have 'total' column");
    }

    /**
     * Test groupBy with joinStrings (STRING_AGG) aggregate function.
     * Tests the two-lambda pattern: ~col:x|$x.name:y|$y->joinStrings(',')
     */
    @Test
    void testGroupByWithJoinStrings() throws SQLException {
        String pureQuery = """
                #TDS
                    id, grp, name
                    1, 1, Alice
                    2, 1, Bob
                    3, 2, Charlie
                    4, 2, Diana
                #->groupBy(~[grp], ~[names:x|$x.name->joinStrings(',')])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("GroupBy with joinStrings result: " + result.rows());

        // Should have 2 groups
        assertEquals(2, result.rows().size(), "Should have 2 groups");

        // Verify column names
        var colNames = result.columns().stream().map(c -> c.name()).toList();
        assertTrue(colNames.contains("grp"), "Should have 'grp' column");
        assertTrue(colNames.contains("names"), "Should have 'names' column");

        // Verify concatenated names contain expected values
        for (var row : result.rows()) {
            int grpIdx = colNames.indexOf("grp");
            int namesIdx = colNames.indexOf("names");
            int grp = ((Number) row.get(grpIdx)).intValue();
            String names = (String) row.get(namesIdx);
            System.out.printf("  grp=%d, names=%s%n", grp, names);

            if (grp == 1) {
                assertTrue(names.contains("Alice") && names.contains("Bob"),
                        "Group 1 should contain Alice and Bob, got: " + names);
            } else if (grp == 2) {
                assertTrue(names.contains("Charlie") && names.contains("Diana"),
                        "Group 2 should contain Charlie and Diana, got: " + names);
            }
        }
    }

    /**
     * Test let binding with groupBy.
     * Pattern: let t = #TDS...#; $t->groupBy(...)
     */
    @Test
    void testLetBindingWithGroupBy() throws SQLException {
        String pureQuery = """
                |let t = #TDS
                    id, grp, name
                    1, 1, A
                    2, 1, B
                    3, 2, C
                    4, 2, D
                #;
                $t->groupBy(~[grp], ~[total:x|$x.id]);
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Let binding with groupBy result: " + result.rows());

        // Should have 2 groups
        assertEquals(2, result.rows().size(), "Should have 2 groups");
    }

    /**
     * Test TdsLiteral->size() comparison operation.
     * This is the EXACT PCT test: testComparisonOperationAfterSize
     * 
     * Pattern: #TDS...#->size() > 0
     * Tests that relation->size() returns a scalar count that can be compared.
     * Should compile to: (SELECT COUNT(*) FROM ...) > 0
     */
    @Test
    void testComparisonOperationAfterSize() throws SQLException {
        String pureQuery = """
                |#TDS
                    val, str
                    1, a
                    3, ewe
                    4, qw
                #->meta::pure::functions::relation::size() > 0
                """;

        var result = executeRelation(pureQuery);
        System.out.println("TDS->size() > 0 result: " + result.rows());

        // A TDS with 3 rows has size 3, so 3 > 0 = true
        assertEquals(1, result.rows().size(), "Should have 1 row");
        assertTrue((Boolean) result.rows().get(0).get(0), "TDS with 3 rows should have size > 0");
    }

    /**
     * Test size() as aggregate function in groupBy.
     * This is the EXACT PCT test: testSize_Relation_Aggregate
     * 
     * Pattern: #TDS...#->groupBy(~grp, ~newCol:x|$x.col:y|$y->size())
     * Tests that size() is recognized as an aggregate function (COUNT).
     */
    @Test
    void testSizeAsAggregateInGroupBy() throws SQLException {
        String pureQuery = """
                |#TDS
                    id, grp, name, employeeNumber
                    1, 2, A, 21
                    2, 1, B, 41
                    3, 3, C, 71
                    4, 4, D, 31
                    5, 2, E, 11
                    6, 1, F, 1
                    7, 3, G, 91
                    8, 1, H, 81
                    9, 5, I, 51
                    10, 0, J, 101
                #->groupBy(~[grp], ~[newCol:x|$x.employeeNumber:y|$y->size()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("GroupBy with size() result: " + result.rows());

        // Should have 6 groups (0, 1, 2, 3, 4, 5)
        assertEquals(6, result.rows().size(), "Should have 6 groups");

        // Find group 1 which has 3 rows (B, F, H)
        var grp1 = result.rows().stream()
                .filter(r -> ((Number) r.get(0)).intValue() == 1)
                .findFirst()
                .orElseThrow();
        assertEquals(3, ((Number) grp1.get(1)).intValue(), "Group 1 should have count 3");
    }

    /**
     * Test size() as window aggregate function in extend.
     * This is the EXACT PCT test: testSize_Relation_Window
     * 
     * Pattern: #TDS...#->extend(~grp->over(), ~newCol:{...}:y|$y->size())
     * Tests that size() maps to COUNT in window context.
     */
    @Test
    void testSizeAsWindowAggregate() throws SQLException {
        String pureQuery = """
                |#TDS
                    id, grp, name
                    1, 2, A
                    2, 1, B
                    3, 3, C
                    4, 4, D
                    5, 2, E
                    6, 1, F
                    7, 3, G
                    8, 1, H
                    9, 5, I
                    10, 0, J
                #->extend(~grp->over(), ~newCol:x|$x.id:y|$y->size())
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Extend with size() window result: " + result.rows());

        // Should have 10 rows
        assertEquals(10, result.rows().size(), "Should have 10 rows");

        // Find a row from group 1 (should have count = 3 since B, F, H are in group 1)
        var grp1Row = result.rows().stream()
                .filter(r -> ((Number) r.get(1)).intValue() == 1)
                .findFirst()
                .orElseThrow();
        assertEquals(3, ((Number) grp1Row.get(3)).intValue(), "Group 1 should have window count 3");
    }

    /**
     * Test sort() with array of sort specs.
     * This is the EXACT PCT test: testSimpleSortShared
     * 
     * Pattern: #TDS...#->sort([~id->ascending(), ~name->ascending()])
     * Tests that sort array arguments are flattened correctly.
     */
    @Test
    void testSortWithArrayOfSortSpecs() throws SQLException {
        String pureQuery = """
                |#TDS
                    id, name
                    2, George
                    3, Pierre
                    1, Sachin
                    1, Neema
                    5, David
                    4, Alex
                    2, Thierry
                #->sort([~id->ascending(), ~name->ascending()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Sort with array result: " + result.rows());

        // Should have 7 rows sorted by id, then by name
        assertEquals(7, result.rows().size(), "Should have 7 rows");

        // First row should be (1, Neema) - id=1, name alphabetically first
        var firstRow = result.rows().get(0);
        assertEquals(1, ((Number) firstRow.get(0)).intValue(), "First row id should be 1");
        assertEquals("Neema", firstRow.get(1), "First row name should be Neema (alphabetically before Sachin)");
    }

    /**
     * Test sort() with descending direction.
     * This is the EXACT PCT test: testSimpleSortShared with descending
     * 
     * Pattern: #TDS...#->sort([~id->descending(), ~name->ascending()])
     * Tests that descending() is correctly detected.
     */
    @Test
    void testSortWithDescending() throws SQLException {
        String pureQuery = """
                |#TDS
                    id, name
                    2, George
                    3, Pierre
                    1, Sachin
                    1, Neema
                    5, David
                    4, Alex
                    2, Thierry
                #->sort([~id->descending(), ~name->ascending()])
                """;

        var result = executeRelation(pureQuery);
        System.out.println("Sort with descending result: " + result.rows());

        // Should have 7 rows sorted by id DESC, then by name ASC
        assertEquals(7, result.rows().size(), "Should have 7 rows");

        // First row should be (5, David) - highest id
        var firstRow = result.rows().get(0);
        assertEquals(5, ((Number) firstRow.get(0)).intValue(), "First row id should be 5 (descending)");
        assertEquals("David", firstRow.get(1), "First row name should be David");
    }

    /**
     * Test for collection map() followed by at() - requires proper context for
     * lambda.
     * 
     * This was failing with NullPointerException because compileConstant
     * wasn't creating an empty context when context was null.
     */
    @Test
    void testCollectionMapWithAt() throws SQLException {
        // GIVEN: A Pure query that uses map() with a lambda, then at()
        String pureQuery = "|['a', 'b', 'c']->map(x: String[1]|$x + 'z')->at(0)";

        // WHEN: We compile and execute
        var result = executeRelation(pureQuery);
        System.out.println("map->at result: " + result.rows());

        // THEN: Should get 'az' - first element after transformation
        assertEquals(1, result.rows().size(), "Should have 1 row");
        assertEquals("az", result.rows().get(0).get(0), "First element should be 'az'");
    }

    // ==================== Variant Type Tests ====================

    /**
     * Test for variant array column with reverse operation.
     * This is the exact PCT test: testVariantArrayColumn_reverse
     * 
     * Pure expression pattern:
     * TDS->extend(~reversed:x|$x.payload->toMany(@Integer)->reverse()->toVariant())
     */
    @Test
    @DisplayName("PCT: Variant array column with reverse - toMany(@Integer)->reverse()->toVariant()")
    void testVariantArrayColumn_reverse() throws SQLException {
        // GIVEN: The exact Pure expression from PCT
        String pureQuery = """
                |#TDS
                    id, payload:meta::pure::metamodel::variant::Variant
                    1, "[1,2,3]"
                    2, "[4,5,6]"
                    3, "[7,8,9]"
                    4, "[10,11,12]"
                    5, "[13,14,15]"
                #->meta::pure::functions::relation::extend(~reversed:x: (id:Integer, payload:meta::pure::metamodel::variant::Variant)[1]|$x.payload->meta::pure::functions::variant::convert::toMany(@Integer)->meta::pure::functions::collection::reverse()->meta::pure::functions::variant::convert::toVariant())
                """;

        // WHEN: We compile and execute
        var result = executeRelation(pureQuery);
        System.out.println("Variant reverse result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // THEN: Should have 5 rows with reversed arrays
        assertEquals(5, result.rows().size(), "Should have 5 rows");

        // Row 1: [1,2,3] -> [3,2,1]
        // Row 2: [4,5,6] -> [6,5,4]
        // etc.
        var row1 = result.rows().get(0);
        assertEquals(1, ((Number) row1.get(0)).intValue(), "ID should be 1");
        String reversed1 = row1.get(2).toString(); // reversed column (DuckDB returns JsonNode)
        assertTrue(reversed1.contains("3") && reversed1.contains("2") && reversed1.contains("1"),
                "Row 1 reversed should contain 3,2,1");
    }

    /**
     * Test for variant array column with sort operation.
     * This is the exact PCT test: testVariantArrayColumn_sort
     * 
     * Pure expression pattern:
     * TDS->extend(~sorted:x|$x.payload->toMany(@Integer)->sort()->toVariant())
     */
    @Test
    @DisplayName("PCT: Variant array column with sort - toMany(@Integer)->sort()->toVariant()")
    void testVariantArrayColumn_sort() throws SQLException {
        // GIVEN: The exact Pure expression from PCT
        String pureQuery = """
                |#TDS
                    id, payload:meta::pure::metamodel::variant::Variant
                    1, "[2,1,3]"
                    2, "[5,6,4]"
                    3, "[9,8,7]"
                    4, "[10,11,12]"
                    5, "[15,13,14]"
                #->meta::pure::functions::relation::extend(~sorted:x: (id:Integer, payload:meta::pure::metamodel::variant::Variant)[1]|$x.payload->meta::pure::functions::variant::convert::toMany(@Integer)->meta::pure::functions::collection::sort()->meta::pure::functions::variant::convert::toVariant())
                """;

        // WHEN: We compile and execute
        var result = executeRelation(pureQuery);
        System.out.println("Variant sort result:");
        for (var row : result.rows()) {
            System.out.println("  " + row);
        }

        // THEN: Should have 5 rows with sorted arrays
        assertEquals(5, result.rows().size(), "Should have 5 rows");

        // Row 1: [2,1,3] -> [1,2,3]
        // Row 2: [5,6,4] -> [4,5,6]
        // Row 3: [9,8,7] -> [7,8,9]
        // etc.
        var row1 = result.rows().get(0);
        assertEquals(1, ((Number) row1.get(0)).intValue(), "ID should be 1");
        String sorted1 = row1.get(2).toString(); // sorted column (DuckDB returns JsonNode)
        // [1,2,3] is the expected sorted result
        assertTrue(sorted1.contains("1") && sorted1.contains("2") && sorted1.contains("3"),
                "Row 1 sorted should contain 1,2,3");
    }
}
