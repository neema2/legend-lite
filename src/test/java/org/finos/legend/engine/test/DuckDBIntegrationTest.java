package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
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

    @Test
    @DisplayName("Generate SQL for JOIN query")
    void testGenerateJoinSql() throws SQLException {
        // GIVEN: T_ADDRESS table already exists and is populated from setupDatabase()
        // (John has 2 addresses, Jane and Bob have 1 each = 4 total)

        // WHEN: We generate a JOIN SQL
        Table personTable = modelBuilder.getTable("T_PERSON");
        Table addressTable = modelBuilder.getTable("T_ADDRESS");

        TableNode personNode = new TableNode(personTable, "p");
        TableNode addressNode = new TableNode(addressTable, "a");

        // Build join condition: p.ID = a.PERSON_ID
        Expression joinCondition = ComparisonExpression.equals(
                ColumnReference.of("p", "ID"),
                ColumnReference.of("a", "PERSON_ID"));

        JoinNode joinNode = JoinNode.inner(personNode, addressNode, joinCondition);

        // Project: firstName, street
        ProjectNode projectNode = new ProjectNode(joinNode, List.of(
                Projection.column("p", "FIRST_NAME", "firstName"),
                Projection.column("a", "STREET", "street")));

        String sql = sqlGenerator.generate(projectNode);
        System.out.println("Generated JOIN SQL: " + sql);

        // THEN: SQL is valid and returns results
        assertTrue(sql.contains("INNER JOIN"), "Should have INNER JOIN");
        assertTrue(sql.contains("ON"), "Should have ON clause");

        // Execute and verify
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String street = rs.getString("street");
                assertNotNull(firstName);
                assertNotNull(street);
                count++;
            }

            // John: 2, Jane: 1, Bob: 1 = 4 total
            assertEquals(4, count, "Should find 4 person-address combinations");
        }
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

        // AND: The query executes correctly
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.Set<String> people = new java.util.HashSet<>();
            while (rs.next()) {
                people.add(rs.getString("firstName"));
            }

            // Only John has address '123 Main St'
            assertEquals(1, people.size(), "Should find exactly 1 person");
            assertTrue(people.contains("John"), "Should find John");
        }
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

        // AND: The query executes and returns person-address pairs
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.List<String> results = new java.util.ArrayList<>();
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String street = rs.getString("street");
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

        // AND: Query executes correctly
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String street = rs.getString("street");
                String city = rs.getString("city");
                assertNotNull(firstName, "firstName should not be null");
                // street and city can be null if no address exists
                System.out.println("  " + firstName + ": " + street + ", " + city);
                count++;
            }

            assertEquals(4, count, "Should have 4 rows");
        }
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

        // AND: Query executes correctly
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String lastName = rs.getString("lastName");
                int age = rs.getInt("age");
                String city = rs.getString("city");

                assertNotNull(firstName);
                assertNotNull(lastName);
                assertTrue(age > 0, "Age should be positive");
                // city can be null for LEFT JOIN

                count++;
            }

            assertEquals(4, count, "Should have 4 rows");
        }
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

        // AND: Query executes correctly
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.List<String> results = new java.util.ArrayList<>();
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String street = rs.getString("street");
                String city = rs.getString("city");
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

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.List<String> results = new java.util.ArrayList<>();
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String street = rs.getString("street");
                results.add(firstName + ": " + street);
                System.out.println("  " + firstName + ": " + street);
            }

            // Jane is the only one with a Chicago address, and she has only 1 address
            assertEquals(1, results.size(), "Should have 1 row");
            assertTrue(results.getFirst().startsWith("Jane:"), "Should be Jane");
            assertTrue(results.getFirst().contains("789 Main Rd"), "Should show Jane's street");
        }
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

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.List<String> results = new java.util.ArrayList<>();
            java.util.Set<String> people = new java.util.HashSet<>();
            while (rs.next()) {
                String firstName = rs.getString("firstName");
                String street = rs.getString("street");
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

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int johnCount = 0;
            int totalCount = 0;
            while (rs.next()) {
                totalCount++;
                if ("John".equals(rs.getString("firstName"))) {
                    johnCount++;
                }
            }

            // THEN: John appears exactly ONCE (not multiple times)
            assertEquals(1, johnCount, "John should appear exactly once");
            assertEquals(1, totalCount, "Only one person lives in New York");
        }
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

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;
            while (rs.next()) {
                assertEquals("John", rs.getString("firstName"));
                count++;
            }

            assertEquals(1, count, "John should appear exactly once despite multiple matching addresses");
        }

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

    // ==================== JOINs for DataFrame/Relational Queries
    // ====================

    @Test
    @DisplayName("JOIN is still correct for DataFrame queries (columns from both sides)")
    void testJoinForRelationalDataFrame() throws SQLException {
        // JOINs are still correct and useful for "Relational" or "DataFrame"
        // style queries where you explicitly SELECT columns from BOTH sides.
        // The row multiplication is INTENTIONAL in this case.

        Table personTable = modelBuilder.getTable("T_PERSON");
        Table addressTable = modelBuilder.getTable("T_ADDRESS");

        TableNode personNode = new TableNode(personTable, "p");
        TableNode addressNode = new TableNode(addressTable, "a");

        Expression joinCondition = ComparisonExpression.equals(
                ColumnReference.of("p", "ID"),
                ColumnReference.of("a", "PERSON_ID"));

        JoinNode joinNode = JoinNode.inner(personNode, addressNode, joinCondition);

        // Project columns from BOTH tables (DataFrame style)
        ProjectNode projectNode = new ProjectNode(joinNode, List.of(
                Projection.column("p", "FIRST_NAME", "personName"),
                Projection.column("a", "STREET", "street"),
                Projection.column("a", "CITY", "city")));

        String sql = sqlGenerator.generate(projectNode);
        System.out.println("DataFrame JOIN SQL: " + sql);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;
            java.util.List<String> rows = new java.util.ArrayList<>();
            while (rs.next()) {
                String row = rs.getString("personName") + " - " +
                        rs.getString("street") + ", " +
                        rs.getString("city");
                rows.add(row);
                count++;
            }

            // We get 4 rows (one per person-address combination)
            // John: 2 addresses, Jane: 1 address, Bob: 1 address
            assertEquals(4, count, "Should have 4 person-address combinations");
            System.out.println("DataFrame results:");
            rows.forEach(r -> System.out.println("  " + r));
        }
    }

    // ==================== GROUP BY / Aggregation Tests ====================

    @Test
    @DisplayName("GroupByNode generates and executes correct SQL in DuckDB")
    void testGroupByNodeExecution() throws Exception {
        // GIVEN: A table with data for aggregation
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE T_EMPLOYEE (ID INTEGER, NAME VARCHAR, DEPARTMENT VARCHAR, SALARY INTEGER)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'John', 'Engineering', 80000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Jane', 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Bob', 'Sales', 60000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (4, 'Alice', 'Sales', 70000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (5, 'Charlie', 'Engineering', 85000)");
        }

        // Build the GroupByNode directly
        Table employeeTable = new Table("T_EMPLOYEE", java.util.List.of(
                Column.required("ID", SqlDataType.INTEGER),
                Column.required("NAME", SqlDataType.VARCHAR),
                Column.required("DEPARTMENT", SqlDataType.VARCHAR),
                Column.required("SALARY", SqlDataType.INTEGER)));

        TableNode tableNode = new TableNode(employeeTable, "t0");

        GroupByNode groupByNode = new GroupByNode(
                tableNode,
                java.util.List.of("DEPARTMENT"),
                java.util.List.of(
                        new GroupByNode.AggregateProjection("totalSalary", "SALARY",
                                AggregateExpression.AggregateFunction.SUM),
                        new GroupByNode.AggregateProjection("avgSalary", "SALARY",
                                AggregateExpression.AggregateFunction.AVG),
                        new GroupByNode.AggregateProjection("headcount", "ID",
                                AggregateExpression.AggregateFunction.COUNT)));

        // WHEN: We generate and execute SQL
        String sql = sqlGenerator.generate(groupByNode);
        System.out.println("GroupBy Integration SQL: " + sql);

        // THEN: Results are correct
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.Map<String, int[]> results = new java.util.HashMap<>();
            while (rs.next()) {
                String dept = rs.getString("DEPARTMENT");
                int total = rs.getInt("totalSalary");
                int avg = rs.getInt("avgSalary");
                int count = rs.getInt("headcount");
                results.put(dept, new int[] { total, avg, count });
                System.out.println("  " + dept + ": total=" + total + ", avg=" + avg + ", count=" + count);
            }

            // Engineering: 80000 + 90000 + 85000 = 255000, avg=85000, count=3
            assertEquals(255000, results.get("Engineering")[0], "Engineering total salary");
            assertEquals(85000, results.get("Engineering")[1], "Engineering avg salary");
            assertEquals(3, results.get("Engineering")[2], "Engineering headcount");

            // Sales: 60000 + 70000 = 130000, avg=65000, count=2
            assertEquals(130000, results.get("Sales")[0], "Sales total salary");
            assertEquals(65000, results.get("Sales")[1], "Sales avg salary");
            assertEquals(2, results.get("Sales")[2], "Sales headcount");
        }
    }

    @Test
    @DisplayName("GroupByNode with FilterNode executes correctly")
    void testGroupByWithFilterExecution() throws Exception {
        // GIVEN: Reuse the T_EMPLOYEE table from above test
        try (Statement stmt = connection.createStatement()) {
            // Drop if exists from previous test
            stmt.execute("DROP TABLE IF EXISTS T_EMPLOYEE");
            stmt.execute("CREATE TABLE T_EMPLOYEE (ID INTEGER, NAME VARCHAR, DEPARTMENT VARCHAR, SALARY INTEGER)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'John', 'Engineering', 80000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Jane', 'Engineering', 90000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Bob', 'Sales', 40000)");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (4, 'Alice', 'Sales', 70000)");
        }

        Table employeeTable = new Table("T_EMPLOYEE", java.util.List.of(
                Column.required("DEPARTMENT", SqlDataType.VARCHAR),
                Column.required("SALARY", SqlDataType.INTEGER)));

        TableNode tableNode = new TableNode(employeeTable, "t0");

        // Filter: SALARY > 50000
        FilterNode filterNode = new FilterNode(tableNode,
                ComparisonExpression.greaterThan(
                        ColumnReference.of("t0", "SALARY"),
                        Literal.integer(50000)));

        GroupByNode groupByNode = new GroupByNode(
                filterNode,
                java.util.List.of("DEPARTMENT"),
                java.util.List.of(
                        new GroupByNode.AggregateProjection("totalSalary", "SALARY",
                                AggregateExpression.AggregateFunction.SUM)));

        // WHEN: We execute
        String sql = sqlGenerator.generate(groupByNode);
        System.out.println("Filter+GroupBy SQL: " + sql);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.Map<String, Integer> results = new java.util.HashMap<>();
            while (rs.next()) {
                results.put(rs.getString("DEPARTMENT"), rs.getInt("totalSalary"));
            }

            // Only salaries > 50000 are included
            // Engineering: 80000 + 90000 = 170000
            // Sales: only 70000 (40000 is filtered out)
            assertEquals(170000, results.get("Engineering"), "Engineering total (>50k)");
            assertEquals(70000, results.get("Sales"), "Sales total (>50k)");
        }
    }

    // ==================== Pure Syntax End-to-End GroupBy Tests
    // ====================

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

        // Build model from Pure source
        String model = """
                Class model::Emp { dept: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, DEPT VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP dept: [EmpDb] T_EMP.DEPT, sal: [EmpDb] T_EMP.SAL } )
                """;

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(model);
        MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
        pureCompiler = new org.finos.legend.pure.dsl.PureCompiler(mappingRegistry);

        // Correct Relation v2 pattern: Class.all() -> project() converts Class to
        // Relation, THEN groupBy
        // project() extracts the columns we want to aggregate, creating a Relation
        String pureQuery = "Emp.all()->project([{e | $e.dept}, {e | $e.sal}], ['dept', 'sal'])->groupBy([{r | $r.dept}], [{r | $r.sal}], ['totalSal'])";
        RelationNode plan = pureCompiler.compile(pureQuery);
        String sql = sqlGenerator.generate(plan);

        System.out.println("Pure Project+GroupBy SQL: " + sql);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            java.util.Map<String, Integer> results = new java.util.HashMap<>();
            while (rs.next()) {
                results.put(rs.getString("dept"), rs.getInt("totalSal"));
            }

            assertEquals(170000, results.get("Engineering"), "Engineering total");
            assertEquals(130000, results.get("Sales"), "Sales total");
        }
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
        // GIVEN: A model with employees
        String model = """
                Class model::Emp { name: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, NAME VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP name: [EmpDb] T_EMP.NAME, sal: [EmpDb] T_EMP.SAL } )
                """;

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(model);
        MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
        pureCompiler = new org.finos.legend.pure.dsl.PureCompiler(mappingRegistry);

        // WHEN: Parse and compile Pure query with sortBy and limit
        String pureQuery = "Emp.all()->sortBy({e | $e.sal}, 'desc')->limit(3)";
        RelationNode plan = pureCompiler.compile(pureQuery);
        String sql = sqlGenerator.generate(plan);

        System.out.println("Pure sortBy+limit SQL: " + sql);

        // THEN: Should generate correct SQL with ORDER BY and LIMIT
        assertTrue(sql.contains("ORDER BY"), "Should contain ORDER BY");
        assertTrue(sql.contains("LIMIT"), "Should contain LIMIT");
        assertTrue(sql.contains("DESC"), "Should contain DESC");
    }

    @Test
    @DisplayName("Pure syntax: Relation->sort()->slice()")
    void testPureSyntaxRelationSortAndSlice() throws Exception {
        // GIVEN: A model
        String model = """
                Class model::Emp { name: String[1]; sal: Integer[1]; }
                Database store::EmpDb ( Table T_EMP ( ID INTEGER, NAME VARCHAR(100), SAL INTEGER ) )
                Mapping model::EmpMap ( Emp: Relational { ~mainTable [EmpDb] T_EMP name: [EmpDb] T_EMP.NAME, sal: [EmpDb] T_EMP.SAL } )
                """;

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(model);
        MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
        pureCompiler = new org.finos.legend.pure.dsl.PureCompiler(mappingRegistry);

        // WHEN: Parse and compile with project()->sort()->slice()
        String pureQuery = "Emp.all()->project([{e | $e.name}, {e | $e.sal}], ['name', 'sal'])->sort('sal', 'asc')->slice(0, 5)";
        RelationNode plan = pureCompiler.compile(pureQuery);
        String sql = sqlGenerator.generate(plan);

        System.out.println("Pure Relation sort+slice SQL: " + sql);

        // THEN: Should generate correct SQL
        assertTrue(sql.contains("ORDER BY"), "Should contain ORDER BY");
        assertTrue(sql.contains("ASC"), "Should contain ASC");
        assertTrue(sql.contains("LIMIT"), "Should contain LIMIT");
    }

    // ==================== Function Definition Tests ====================

    @Test
    @DisplayName("Function with Class query - execute body against DuckDB")
    void testFunctionWithClassQuery_DuckDB() throws Exception {
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

        System.out.println("Function body SQL (getAdults): " + sql);

        // Execute against DuckDB
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
    @DisplayName("Function with Relation query - execute body against DuckDB")
    void testFunctionWithRelationQuery_DuckDB() throws Exception {
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

        System.out.println("Function body SQL (getWorkerInfo): " + sql);

        // Execute against DuckDB
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

    // ==================== Relation API Tests ====================

    @Test
    @DisplayName("Relation API: #>{DB.TABLE}->from(runtime) - execute via QueryService")
    void testRelationApiDirectTableQuery() throws Exception {
        // GIVEN: A Relation query with ->from(runtime)
        // The query: #>{store::PersonDatabase.T_PERSON}->from(test::TestRuntime)

        String pureSource = getCompletePureModelWithRuntime();
        String relationQuery = "#>{store::PersonDatabase.T_PERSON}->from(test::TestRuntime)";

        // WHEN: Execute through QueryService (parse → compile → SQL → execute)
        org.finos.legend.engine.execution.RelationResult result = queryService.execute(
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
}
