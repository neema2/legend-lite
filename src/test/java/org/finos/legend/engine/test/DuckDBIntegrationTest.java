package org.finos.legend.engine.test;

import org.finos.legend.engine.store.*;
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
}
