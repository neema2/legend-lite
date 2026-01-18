package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.dsl.m2m.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.m3.*;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Model-to-Model transforms using real Pure syntax.
 * 
 * These tests verify:
 * - Pure M2M mapping syntax parsing
 * - M2M expression compilation to SQL
 * - Execution against real DuckDB
 * 
 * The tests use REAL Pure syntax that is parsed, not programmatically
 * constructed objects.
 */
@DisplayName("M2M Integration Tests - Real Pure Syntax")
class M2MIntegrationTest {

    private Connection connection;
    private SQLGenerator generator;
    private MappingRegistry mappingRegistry;

    // ==================== Pure Source Definitions ====================

    /**
     * Pure source class: RawPerson with raw data.
     */
    private static final String RAW_PERSON_CLASS = """
            Class model::RawPerson
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
                salary: Float[1];
                isActive: Boolean[1];
            }
            """;

    /**
     * Pure database definition for the raw source table.
     */
    private static final String RAW_PERSON_DATABASE = """
            Database store::RawDatabase
            (
                Table T_RAW_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE INTEGER NOT NULL,
                    SALARY DECIMAL(10,2) NOT NULL,
                    IS_ACTIVE BOOLEAN NOT NULL
                )
            )
            """;

    /**
     * Pure relational mapping for RawPerson class.
     */
    private static final String RAW_PERSON_MAPPING = """
            Mapping model::RawMapping
            (
                RawPerson: Relational
                {
                    ~mainTable [RawDatabase] T_RAW_PERSON
                    firstName: [RawDatabase] T_RAW_PERSON.FIRST_NAME,
                    lastName: [RawDatabase] T_RAW_PERSON.LAST_NAME,
                    age: [RawDatabase] T_RAW_PERSON.AGE,
                    salary: [RawDatabase] T_RAW_PERSON.SALARY,
                    isActive: [RawDatabase] T_RAW_PERSON.IS_ACTIVE
                }
            )
            """;

    /**
     * Pure M2M mapping from RawPerson to Person (transformation layer).
     */
    private static final String M2M_PERSON_MAPPING = """
            Mapping model::PersonM2MMapping
            (
                Person: Pure
                {
                    ~src RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName,
                    upperLastName: $src.lastName->toUpper()
                }
            )
            """;

    /**
     * Pure M2M mapping with conditional expression.
     */
    private static final String M2M_CONDITIONAL_MAPPING = """
            Mapping model::ConditionalMapping
            (
                PersonView: Pure
                {
                    ~src RawPerson
                    firstName: $src.firstName,
                    ageGroup: if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))
                }
            )
            """;

    /**
     * Pure M2M mapping with filter.
     */
    private static final String M2M_FILTERED_MAPPING = """
            Mapping model::FilteredMapping
            (
                ActivePerson: Pure
                {
                    ~src RawPerson
                    ~filter $src.isActive == true
                    firstName: $src.firstName,
                    lastName: $src.lastName
                }
            )
            """;

    /**
     * Pure M2M mapping with salary band.
     */
    private static final String M2M_SALARY_BAND_MAPPING = """
            Mapping model::SalaryBandMapping
            (
                PersonWithSalary: Pure
                {
                    ~src RawPerson
                    firstName: $src.firstName,
                    salaryBand: if($src.salary < 50000, |'Entry', |if($src.salary < 100000, |'Mid', |'Senior'))
                }
            )
            """;

    // Relational mapping components
    private PureClass rawPersonClass;
    private Table rawPersonTable;
    private RelationalMapping rawPersonMapping;

    @BeforeEach
    void setUp() throws SQLException {
        // Create in-memory DuckDB
        connection = DriverManager.getConnection("jdbc:duckdb:");
        generator = new SQLGenerator(DuckDBDialect.INSTANCE);

        // Create source table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_RAW_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE INTEGER,
                        SALARY DECIMAL(10,2),
                        IS_ACTIVE BOOLEAN
                    )
                    """);

            // Insert test data
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (1, 'John', 'Smith', 30, 75000.00, true)");
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (2, 'Jane', 'Doe', 25, 55000.00, true)");
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (3, 'Bob', 'Jones', 45, 120000.00, false)");
            stmt.execute("INSERT INTO T_RAW_PERSON VALUES (4, 'Alice', 'Wonder', 17, 0.00, true)");
        }

        // Build model from Pure definitions
        PureModelBuilder modelBuilder = new PureModelBuilder()
                .addSource(RAW_PERSON_CLASS)
                .addSource(RAW_PERSON_DATABASE)
                .addSource(RAW_PERSON_MAPPING);

        mappingRegistry = modelBuilder.getMappingRegistry();
        rawPersonClass = modelBuilder.getClass("RawPerson");
        rawPersonTable = modelBuilder.getTable("T_RAW_PERSON");
        rawPersonMapping = mappingRegistry.getByClassName("RawPerson");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // ==================== Pure Syntax Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure M2M mapping syntax")
    void testParseM2MMapping() {
        // WHEN: We parse the M2M mapping
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_PERSON_MAPPING);

        // THEN: We get a valid definition
        assertEquals("model::PersonM2MMapping", mappingDef.qualifiedName());
        assertEquals(1, mappingDef.classMappings().size());

        var classMapping = mappingDef.classMappings().getFirst();
        assertEquals("Person", classMapping.targetClassName());
        assertEquals("RawPerson", classMapping.sourceClassName());
        assertEquals(2, classMapping.propertyMappings().size());

        // Verify expressions
        var fullNameMapping = classMapping.findPropertyMapping("fullName").orElseThrow();
        assertEquals("$src.firstName + ' ' + $src.lastName", fullNameMapping.expressionString());

        var upperLastNameMapping = classMapping.findPropertyMapping("upperLastName").orElseThrow();
        assertEquals("$src.lastName->toUpper()", upperLastNameMapping.expressionString());
    }

    @Test
    @DisplayName("Parse Pure M2M mapping with filter")
    void testParseM2MWithFilter() {
        // WHEN: We parse the filtered mapping
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_FILTERED_MAPPING);

        // THEN: Filter is extracted
        var classMapping = mappingDef.classMappings().getFirst();
        assertTrue(classMapping.optionalFilterExpression().isPresent());
        assertEquals("$src.isActive == true", classMapping.optionalFilterExpression().get());
    }

    @Test
    @DisplayName("Parse Pure M2M mapping with conditional")
    void testParseM2MWithConditional() {
        // WHEN: We parse the conditional mapping
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_CONDITIONAL_MAPPING);

        // THEN: Conditional expression is extracted
        var classMapping = mappingDef.classMappings().getFirst();
        var ageGroupMapping = classMapping.findPropertyMapping("ageGroup").orElseThrow();
        String expr = ageGroupMapping.expressionString();

        assertTrue(expr.contains("if("), "Should contain if expression");
        assertTrue(expr.contains("$src.age < 18"), "Should contain age condition");
        assertTrue(expr.contains("Minor"), "Should contain Minor branch");
    }

    // ==================== Expression Parser Tests ====================

    @Test
    @DisplayName("Parse simple property reference: $src.firstName")
    void testParseSimpleProperty() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName");

        assertInstanceOf(SourcePropertyRef.class, expr);
        SourcePropertyRef ref = (SourcePropertyRef) expr;
        assertEquals(List.of("firstName"), ref.propertyChain());
    }

    @Test
    @DisplayName("Parse string concatenation: $src.firstName + ' ' + $src.lastName")
    void testParseStringConcat() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName + ' ' + $src.lastName");

        assertInstanceOf(StringConcatExpr.class, expr);
        StringConcatExpr concat = (StringConcatExpr) expr;
        assertEquals(3, concat.parts().size());
    }

    @Test
    @DisplayName("Parse function call: $src.firstName->toUpper()")
    void testParseFunctionCall() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName->toUpper()");

        assertInstanceOf(FunctionCallExpr.class, expr);
        FunctionCallExpr func = (FunctionCallExpr) expr;
        assertEquals("toUpper", func.functionName());
        assertInstanceOf(SourcePropertyRef.class, func.target());
    }

    @Test
    @DisplayName("Parse chained function calls: $src.firstName->trim()->toUpper()")
    void testParseChainedFunctions() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName->trim()->toUpper()");

        assertInstanceOf(FunctionCallExpr.class, expr);
        FunctionCallExpr outer = (FunctionCallExpr) expr;
        assertEquals("toUpper", outer.functionName());

        assertInstanceOf(FunctionCallExpr.class, outer.target());
        FunctionCallExpr inner = (FunctionCallExpr) outer.target();
        assertEquals("trim", inner.functionName());
    }

    @Test
    @DisplayName("Parse if expression: if($src.age < 18, |'Minor', |'Adult')")
    void testParseIfExpression() {
        M2MExpression expr = M2MExpressionParser.parse("if($src.age < 18, |'Minor', |'Adult')");

        assertInstanceOf(IfExpr.class, expr);
        IfExpr ifExpr = (IfExpr) expr;

        assertInstanceOf(M2MComparisonExpr.class, ifExpr.condition());
        assertInstanceOf(M2MLiteral.class, ifExpr.thenBranch());
        assertInstanceOf(M2MLiteral.class, ifExpr.elseBranch());

        M2MLiteral thenLit = (M2MLiteral) ifExpr.thenBranch();
        assertEquals("Minor", thenLit.value());
    }

    // ==================== SQL Generation Tests ====================

    @Test
    @DisplayName("Compile simple property mapping to SQL")
    void testCompileSimpleProperty() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName");

        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);

        String sql = generator.generateExpression(sqlExpr);
        assertEquals("\"t0\".\"FIRST_NAME\"", sql);
    }

    @Test
    @DisplayName("Compile string concatenation to SQL")
    void testCompileStringConcat() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName + ' ' + $src.lastName");

        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);

        String sql = generator.generateExpression(sqlExpr);
        assertEquals("(\"t0\".\"FIRST_NAME\" || ' ' || \"t0\".\"LAST_NAME\")", sql);
    }

    @Test
    @DisplayName("Compile toUpper function to SQL")
    void testCompileToUpper() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName->toUpper()");

        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);

        String sql = generator.generateExpression(sqlExpr);
        assertEquals("UPPER(\"t0\".\"FIRST_NAME\")", sql);
    }

    @Test
    @DisplayName("Compile if expression to CASE WHEN SQL")
    void testCompileIfToCaseWhen() {
        M2MExpression expr = M2MExpressionParser.parse("if($src.age < 18, |'Minor', |'Adult')");

        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);

        String sql = generator.generateExpression(sqlExpr);
        assertEquals("CASE WHEN \"t0\".\"AGE\" < 18 THEN 'Minor' ELSE 'Adult' END", sql);
    }

    // ==================== Full M2M Mapping Tests with Real DuckDB
    // ====================

    @Test
    @DisplayName("Compile full M2M mapping from Pure syntax and execute against DuckDB")
    void testFullM2MMapping() throws SQLException {
        // GIVEN: Parse the M2M mapping from Pure syntax
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_PERSON_MAPPING);
        var classMappingDef = mappingDef.classMappings().getFirst();

        // Build M2MClassMapping from parsed definition
        M2MClassMapping classMapping = new M2MClassMapping(
                classMappingDef.targetClassName(),
                null,
                classMappingDef.sourceClassName(),
                null, // no filter
                classMappingDef.propertyMappings().stream()
                        .map(pm -> new M2MPropertyMapping(
                                pm.propertyName(),
                                M2MExpressionParser.parse(pm.expressionString())))
                        .toList());

        // WHEN: Compile and generate SQL
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(classMapping);
        String sql = generator.generate(plan);

        System.out.println("Generated SQL: " + sql);

        // THEN: Execute and verify results
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals("John Smith", rs.getString("fullName"));
            assertEquals("SMITH", rs.getString("upperLastName"));

            assertTrue(rs.next());
            assertEquals("Jane Doe", rs.getString("fullName"));
            assertEquals("DOE", rs.getString("upperLastName"));
        }
    }

    @Test
    @DisplayName("M2M with conditional expression (age groups) - DuckDB")
    void testM2MWithConditional() throws SQLException {
        // GIVEN: Parse the conditional M2M mapping
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_CONDITIONAL_MAPPING);
        var classMappingDef = mappingDef.classMappings().getFirst();

        M2MClassMapping classMapping = new M2MClassMapping(
                classMappingDef.targetClassName(),
                null,
                classMappingDef.sourceClassName(),
                null,
                classMappingDef.propertyMappings().stream()
                        .map(pm -> new M2MPropertyMapping(
                                pm.propertyName(),
                                M2MExpressionParser.parse(pm.expressionString())))
                        .toList());

        // WHEN: Compile and generate SQL
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(classMapping);
        String sql = generator.generate(plan);

        System.out.println("Generated SQL: " + sql);

        // THEN: Execute and verify
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            // ID 1: John, age 30 -> Adult
            assertTrue(rs.next());
            assertEquals("John", rs.getString("firstName"));
            assertEquals("Adult", rs.getString("ageGroup"));

            // ID 2: Jane, age 25 -> Adult
            assertTrue(rs.next());
            assertEquals("Jane", rs.getString("firstName"));
            assertEquals("Adult", rs.getString("ageGroup"));

            // ID 3: Bob, age 45 -> Adult
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("firstName"));
            assertEquals("Adult", rs.getString("ageGroup"));

            // ID 4: Alice, age 17 -> Minor
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("firstName"));
            assertEquals("Minor", rs.getString("ageGroup"));
        }
    }

    @Test
    @DisplayName("M2M with filter (active only) - DuckDB")
    void testM2MWithFilter() throws SQLException {
        // GIVEN: Parse the filtered M2M mapping
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_FILTERED_MAPPING);
        var classMappingDef = mappingDef.classMappings().getFirst();

        M2MExpression filter = classMappingDef.optionalFilterExpression()
                .map(M2MExpressionParser::parse)
                .orElse(null);

        M2MClassMapping classMapping = new M2MClassMapping(
                classMappingDef.targetClassName(),
                null,
                classMappingDef.sourceClassName(),
                filter,
                classMappingDef.propertyMappings().stream()
                        .map(pm -> new M2MPropertyMapping(
                                pm.propertyName(),
                                M2MExpressionParser.parse(pm.expressionString())))
                        .toList());

        // WHEN: Compile and generate SQL
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(classMapping);
        String sql = generator.generate(plan);

        System.out.println("Generated SQL: " + sql);

        // THEN: Execute and verify - should only return active people (not Bob)
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;
            while (rs.next()) {
                String lastName = rs.getString("lastName");
                assertNotEquals("Jones", lastName, "Bob Jones should be filtered out (inactive)");
                count++;
            }
            assertEquals(3, count, "Should have 3 active people");
        }
    }

    @Test
    @DisplayName("M2M with salary band conditional - DuckDB")
    void testM2MWithSalaryBand() throws SQLException {
        // GIVEN: Parse the salary band M2M mapping
        M2MMappingDefinition mappingDef = PureDefinitionParser.parseM2MMappingDefinition(M2M_SALARY_BAND_MAPPING);
        var classMappingDef = mappingDef.classMappings().getFirst();

        M2MClassMapping classMapping = new M2MClassMapping(
                classMappingDef.targetClassName(),
                null,
                classMappingDef.sourceClassName(),
                null,
                classMappingDef.propertyMappings().stream()
                        .map(pm -> new M2MPropertyMapping(
                                pm.propertyName(),
                                M2MExpressionParser.parse(pm.expressionString())))
                        .toList());

        // WHEN: Compile and generate SQL
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(classMapping);
        String sql = generator.generate(plan);

        System.out.println("Generated SQL: " + sql);

        // THEN: Execute and verify
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            // John: 75000 -> Mid
            assertTrue(rs.next());
            assertEquals("John", rs.getString("firstName"));
            assertEquals("Mid", rs.getString("salaryBand"));

            // Jane: 55000 -> Mid
            assertTrue(rs.next());
            assertEquals("Jane", rs.getString("firstName"));
            assertEquals("Mid", rs.getString("salaryBand"));

            // Bob: 120000 -> Senior
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("firstName"));
            assertEquals("Senior", rs.getString("salaryBand"));

            // Alice: 0 -> Entry
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("firstName"));
            assertEquals("Entry", rs.getString("salaryBand"));
        }
    }
}
