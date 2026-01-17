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
        
        // THEN: We get 3 definitions
        assertEquals(3, definitions.size());
        assertInstanceOf(ClassDefinition.class, definitions.get(0));
        assertInstanceOf(DatabaseDefinition.class, definitions.get(1));
        assertInstanceOf(MappingDefinition.class, definitions.get(2));
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
        // GIVEN: Set up tables with foreign key relationship
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE T_ADDRESS (
                    ID INTEGER PRIMARY KEY,
                    PERSON_ID INTEGER NOT NULL,
                    STREET VARCHAR(200) NOT NULL,
                    CITY VARCHAR(100) NOT NULL
                )
                """);
            
            // Insert test addresses
            stmt.execute("INSERT INTO T_ADDRESS VALUES (1, 1, '123 Main St', 'New York')");
            stmt.execute("INSERT INTO T_ADDRESS VALUES (2, 1, '456 Oak Ave', 'Boston')");
            stmt.execute("INSERT INTO T_ADDRESS VALUES (3, 2, '789 Pine Rd', 'Chicago')");
        }
        
        // WHEN: We generate a JOIN SQL
        Table personTable = modelBuilder.getTable("T_PERSON");
        Table addressTable = new Table("T_ADDRESS", List.of(
                Column.required("ID", SqlDataType.INTEGER),
                Column.required("PERSON_ID", SqlDataType.INTEGER),
                Column.required("STREET", SqlDataType.VARCHAR),
                Column.required("CITY", SqlDataType.VARCHAR)
        ));
        
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
            
            assertEquals(3, count, "Should find 3 person-address combinations");
        }
    }
}
