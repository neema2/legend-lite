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
 * - Class definitions: {@code Class package::Name { property: Type[multiplicity]; }}
 * - Database definitions: {@code Database package::Name ( Table ... )}
 * - Mapping definitions: {@code Mapping package::Name ( ClassName: Relational { ... } )}
 * - Query expressions: {@code ClassName.all()->filter({p | ...})->project(...)}
 */
@DisplayName("DuckDB Integration Tests - Full Pure Language")
class DuckDBIntegrationTest extends AbstractDatabaseTest {
    
    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }
    
    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:";  // In-memory DuckDB
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
    @DisplayName("Parse Pure Class definition")
    void testParseClassDefinition() {
        // GIVEN: A Pure Class definition
        String pureClass = """
            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }
            """;
        
        // WHEN: We parse it
        ClassDefinition classDef = PureDefinitionParser.parseClassDefinition(pureClass);
        
        // THEN: We get a valid ClassDefinition
        assertEquals("model::Person", classDef.qualifiedName());
        assertEquals("Person", classDef.simpleName());
        assertEquals("model", classDef.packagePath());
        assertEquals(3, classDef.properties().size());
        
        // Verify properties
        var firstName = classDef.properties().get(0);
        assertEquals("firstName", firstName.name());
        assertEquals("String", firstName.type());
        assertEquals(1, firstName.lowerBound());
        assertEquals(Integer.valueOf(1), firstName.upperBound());
        
        var age = classDef.properties().get(2);
        assertEquals("age", age.name());
        assertEquals("Integer", age.type());
    }
    
    @Test
    @DisplayName("Parse Pure Database definition")
    void testParseDatabaseDefinition() {
        // GIVEN: A Pure Database definition
        String pureDatabase = """
            Database store::MyDatabase
            (
                Table T_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE_VAL INTEGER
                )
            )
            """;
        
        // WHEN: We parse it
        DatabaseDefinition dbDef = PureDefinitionParser.parseDatabaseDefinition(pureDatabase);
        
        // THEN: We get a valid DatabaseDefinition
        assertEquals("store::MyDatabase", dbDef.qualifiedName());
        assertEquals("MyDatabase", dbDef.simpleName());
        assertEquals(1, dbDef.tables().size());
        
        // Verify table
        var table = dbDef.tables().getFirst();
        assertEquals("T_PERSON", table.name());
        assertEquals(4, table.columns().size());
        
        // Verify columns
        var idCol = table.columns().get(0);
        assertEquals("ID", idCol.name());
        assertEquals("INTEGER", idCol.dataType());
        assertTrue(idCol.primaryKey());
        
        var firstNameCol = table.columns().get(1);
        assertEquals("FIRST_NAME", firstNameCol.name());
        assertTrue(firstNameCol.notNull());
        assertFalse(firstNameCol.primaryKey());
    }
    
    @Test
    @DisplayName("Parse Pure Mapping definition")
    void testParseMappingDefinition() {
        // GIVEN: A Pure Mapping definition
        String pureMapping = """
            Mapping model::PersonMapping
            (
                Person: Relational
                {
                    ~mainTable [MyDatabase] T_PERSON
                    firstName: [MyDatabase] T_PERSON.FIRST_NAME,
                    lastName: [MyDatabase] T_PERSON.LAST_NAME,
                    age: [MyDatabase] T_PERSON.AGE_VAL
                }
            )
            """;
        
        // WHEN: We parse it
        MappingDefinition mappingDef = PureDefinitionParser.parseMappingDefinition(pureMapping);
        
        // THEN: We get a valid MappingDefinition
        assertEquals("model::PersonMapping", mappingDef.qualifiedName());
        assertEquals("PersonMapping", mappingDef.simpleName());
        assertEquals(1, mappingDef.classMappings().size());
        
        // Verify class mapping
        var classMapping = mappingDef.classMappings().getFirst();
        assertEquals("Person", classMapping.className());
        assertEquals("Relational", classMapping.mappingType());
        assertNotNull(classMapping.mainTable());
        assertEquals("MyDatabase", classMapping.mainTable().databaseName());
        assertEquals("T_PERSON", classMapping.mainTable().tableName());
        
        // Verify property mappings
        assertEquals(3, classMapping.propertyMappings().size());
        
        var firstNameMapping = classMapping.propertyMappings().get(0);
        assertEquals("firstName", firstNameMapping.propertyName());
        assertEquals("FIRST_NAME", firstNameMapping.columnReference().columnName());
    }
    
    @Test
    @DisplayName("Parse complete Pure model")
    void testParseCompletePureModel() {
        // GIVEN: The complete Pure model from AbstractDatabaseTest
        
        // WHEN: We parse it
        List<PureDefinition> definitions = PureDefinitionParser.parse(COMPLETE_PURE_MODEL);
        
        // THEN: We get 5 definitions (Person, Address, Association, Database, Mapping)
        assertEquals(5, definitions.size());
        assertInstanceOf(ClassDefinition.class, definitions.get(0)); // Person
        assertInstanceOf(ClassDefinition.class, definitions.get(1)); // Address
        assertInstanceOf(AssociationDefinition.class, definitions.get(2)); // Person_Address
        assertInstanceOf(DatabaseDefinition.class, definitions.get(3)); // PersonDatabase
        assertInstanceOf(MappingDefinition.class, definitions.get(4)); // PersonMapping
    }
    
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
        String sql = compileAndGenerateSql(pureQuery);
        
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
    
    // ==================== Association & Join Tests ====================
    
    @Test
    @DisplayName("Parse Pure Association definition")
    void testParseAssociationDefinition() {
        // GIVEN: A Pure Association definition
        String pureAssociation = """
            Association model::Person_Address
            {
                person: Person[1];
                addresses: Address[*];
            }
            """;
        
        // WHEN: We parse it
        AssociationDefinition assocDef = PureDefinitionParser.parseAssociationDefinition(pureAssociation);
        
        // THEN: We get a valid AssociationDefinition
        assertEquals("model::Person_Address", assocDef.qualifiedName());
        assertEquals("Person_Address", assocDef.simpleName());
        
        // Verify property1 (person -> Person)
        var prop1 = assocDef.property1();
        assertEquals("person", prop1.propertyName());
        assertEquals("Person", prop1.targetClass());
        assertEquals("1", prop1.multiplicityString());
        
        // Verify property2 (addresses -> Address)
        var prop2 = assocDef.property2();
        assertEquals("addresses", prop2.propertyName());
        assertEquals("Address", prop2.targetClass());
        assertEquals("*", prop2.multiplicityString());
    }
    
    @Test
    @DisplayName("Parse Database with Join definition")
    void testParseDatabaseWithJoin() {
        // GIVEN: A Pure Database with tables and a join
        String pureDatabase = """
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
        
        // WHEN: We parse it
        DatabaseDefinition dbDef = PureDefinitionParser.parseDatabaseDefinition(pureDatabase);
        
        // THEN: We get tables and joins
        assertEquals("store::TestDB", dbDef.qualifiedName());
        assertEquals(2, dbDef.tables().size());
        assertEquals(1, dbDef.joins().size());
        
        // Verify join
        var join = dbDef.findJoin("Person_Address").orElseThrow();
        assertEquals("Person_Address", join.name());
        assertEquals("T_PERSON", join.leftTable());
        assertEquals("ID", join.leftColumn());
        assertEquals("T_ADDRESS", join.rightTable());
        assertEquals("PERSON_ID", join.rightColumn());
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
                ColumnReference.of("a", "PERSON_ID")
        );
        
        JoinNode joinNode = JoinNode.inner(personNode, addressNode, joinCondition);
        
        // Project: firstName, street
        ProjectNode projectNode = new ProjectNode(joinNode, List.of(
                Projection.column("p", "FIRST_NAME", "firstName"),
                Projection.column("a", "STREET", "street")
        ));
        
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
    // The compiler automatically generates EXISTS for to-many navigation in filters.
    
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
        String sql = compileAndGenerateSql(pureQuery);
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
        String sql = compileAndGenerateSql(pureQuery);
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
        String sql = compileAndGenerateSql(pureQuery);
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
        String sql = compileAndGenerateSql(pureQuery);
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
        //        and show their name plus ALL their addresses
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
        String sql = compileAndGenerateSql(pureQuery);
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
            // But we should see BOTH of John's addresses (row multiplication from LEFT JOIN)
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
        
        String sql = compileAndGenerateSql(pureQuery);
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
        // This is a simpler case: no EXISTS needed for filter, just LEFT JOIN for projection
        String pureQuery = """
            Person.all()
                ->filter({p | $p.lastName == 'Smith'})
                ->project({p | $p.firstName}, {p | $p.addresses.street})
            """;
        
        String sql = compileAndGenerateSql(pureQuery);
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
        
        // First, let's modify address data so both of John's addresses match a condition
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
        String sql = compileAndGenerateSql(pureQuery);
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
        // The key point is John should appear ONCE even if he has multiple Main addresses.
        
        String sql = compileAndGenerateSql(pureQuery);
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
        String sql = compileAndGenerateSql(pureQuery);
        System.out.println("SQL Structure Test: " + sql);
        
        // THEN: Verify the SQL structure matches expected EXISTS pattern
        // Expected form:
        // SELECT ... FROM "T_PERSON" AS "t0" 
        // WHERE EXISTS (SELECT 1 FROM "T_ADDRESS" AS "sub0" 
        //               WHERE ("sub0"."PERSON_ID" = "t0"."ID" AND "sub0"."STREET" = 'Test St'))
        
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
    
    // ==================== JOINs for DataFrame/Relational Queries ====================
    
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
                ColumnReference.of("a", "PERSON_ID")
        );
        
        JoinNode joinNode = JoinNode.inner(personNode, addressNode, joinCondition);
        
        // Project columns from BOTH tables (DataFrame style)
        ProjectNode projectNode = new ProjectNode(joinNode, List.of(
                Projection.column("p", "FIRST_NAME", "personName"),
                Projection.column("a", "STREET", "street"),
                Projection.column("a", "CITY", "city")
        ));
        
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
}
