package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.m2m.*;
import org.finos.legend.pure.m3.*;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Model-to-Model transforms.
 * 
 * These tests verify that M2M mappings compile to correct SQL
 * and execute correctly against DuckDB.
 */
@DisplayName("M2M Integration Tests")
class M2MIntegrationTest {
    
    private Connection connection;
    private SQLGenerator generator;
    
    // Source model: RawPerson
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
        
        // Define source model
        rawPersonClass = new PureClass("RawPerson", List.of(
                new Property("firstName", PrimitiveType.STRING, Multiplicity.ONE),
                new Property("lastName", PrimitiveType.STRING, Multiplicity.ONE),
                new Property("age", PrimitiveType.INTEGER, Multiplicity.ONE),
                new Property("salary", PrimitiveType.FLOAT, Multiplicity.ONE),
                new Property("isActive", PrimitiveType.BOOLEAN, Multiplicity.ONE)
        ));
        
        rawPersonTable = new Table("", "T_RAW_PERSON", List.of(
                Column.required("ID", SqlDataType.INTEGER),
                Column.required("FIRST_NAME", SqlDataType.VARCHAR),
                Column.required("LAST_NAME", SqlDataType.VARCHAR),
                Column.required("AGE", SqlDataType.INTEGER),
                Column.required("SALARY", SqlDataType.DECIMAL),
                Column.required("IS_ACTIVE", SqlDataType.BOOLEAN)
        ));
        
        rawPersonMapping = new RelationalMapping(rawPersonClass, rawPersonTable, List.of(
                new PropertyMapping("firstName", "FIRST_NAME"),
                new PropertyMapping("lastName", "LAST_NAME"),
                new PropertyMapping("age", "AGE"),
                new PropertyMapping("salary", "SALARY"),
                new PropertyMapping("isActive", "IS_ACTIVE")
        ));
    }
    
    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
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
    
    @Test
    @DisplayName("Parse nested if: if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))")
    void testParseNestedIf() {
        M2MExpression expr = M2MExpressionParser.parse(
                "if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))");
        
        assertInstanceOf(IfExpr.class, expr);
        IfExpr outer = (IfExpr) expr;
        assertInstanceOf(IfExpr.class, outer.elseBranch());
        
        IfExpr inner = (IfExpr) outer.elseBranch();
        assertEquals("Adult", ((M2MLiteral) inner.thenBranch()).value());
        assertEquals("Senior", ((M2MLiteral) inner.elseBranch()).value());
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
    @DisplayName("Compile toLower function to SQL")
    void testCompileToLower() {
        M2MExpression expr = M2MExpressionParser.parse("$src.lastName->toLower()");
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);
        
        String sql = generator.generateExpression(sqlExpr);
        assertEquals("LOWER(\"t0\".\"LAST_NAME\")", sql);
    }
    
    @Test
    @DisplayName("Compile trim function to SQL")
    void testCompileTrim() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName->trim()");
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);
        
        String sql = generator.generateExpression(sqlExpr);
        assertEquals("TRIM(\"t0\".\"FIRST_NAME\")", sql);
    }
    
    @Test
    @DisplayName("Compile chained functions to SQL")
    void testCompileChainedFunctions() {
        M2MExpression expr = M2MExpressionParser.parse("$src.firstName->trim()->toUpper()");
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);
        
        String sql = generator.generateExpression(sqlExpr);
        assertEquals("UPPER(TRIM(\"t0\".\"FIRST_NAME\"))", sql);
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
    
    @Test
    @DisplayName("Compile nested if to flattened CASE WHEN SQL")
    void testCompileNestedIfToFlattenedCase() {
        M2MExpression expr = M2MExpressionParser.parse(
                "if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))");
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        Expression sqlExpr = compiler.compileExpression(expr);
        
        String sql = generator.generateExpression(sqlExpr);
        assertEquals("CASE WHEN \"t0\".\"AGE\" < 18 THEN 'Minor' WHEN \"t0\".\"AGE\" < 65 THEN 'Adult' ELSE 'Senior' END", sql);
    }
    
    // ==================== Full M2M Mapping Tests ====================
    
    @Test
    @DisplayName("Compile full M2M mapping to SQL and execute")
    void testFullM2MMapping() throws SQLException {
        // Define M2M mapping: RawPerson -> Person with fullName derived
        M2MClassMapping mapping = new M2MClassMapping(
                "Person",
                null,
                "RawPerson",
                null,
                List.of(
                        new M2MPropertyMapping("fullName", 
                                M2MExpressionParser.parse("$src.firstName + ' ' + $src.lastName")),
                        new M2MPropertyMapping("upperLastName",
                                M2MExpressionParser.parse("$src.lastName->toUpper()"))
                )
        );
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(mapping);
        
        String sql = generator.generate(plan);
        System.out.println("Generated SQL: " + sql);
        
        // Execute and verify
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
    @DisplayName("M2M with conditional expression (age groups)")
    void testM2MWithConditional() throws SQLException {
        M2MClassMapping mapping = new M2MClassMapping(
                "PersonView",
                null,
                "RawPerson",
                null,
                List.of(
                        new M2MPropertyMapping("firstName",
                                M2MExpressionParser.parse("$src.firstName")),
                        new M2MPropertyMapping("ageGroup",
                                M2MExpressionParser.parse(
                                        "if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))"))
                )
        );
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(mapping);
        
        String sql = generator.generate(plan);
        System.out.println("Generated SQL: " + sql);
        
        // Execute and verify
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
    @DisplayName("M2M with filter (active only)")
    void testM2MWithFilter() throws SQLException {
        M2MClassMapping mapping = new M2MClassMapping(
                "ActivePerson",
                null,
                "RawPerson",
                M2MExpressionParser.parse("$src.isActive == true"),  // Filter
                List.of(
                        new M2MPropertyMapping("firstName",
                                M2MExpressionParser.parse("$src.firstName")),
                        new M2MPropertyMapping("lastName",
                                M2MExpressionParser.parse("$src.lastName"))
                )
        );
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(mapping);
        
        String sql = generator.generate(plan);
        System.out.println("Generated SQL: " + sql);
        
        // Execute and verify - should only return active people (not Bob)
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
    @DisplayName("M2M with salary band conditional")
    void testM2MWithSalaryBand() throws SQLException {
        M2MClassMapping mapping = new M2MClassMapping(
                "PersonWithSalary",
                null,
                "RawPerson",
                null,
                List.of(
                        new M2MPropertyMapping("firstName",
                                M2MExpressionParser.parse("$src.firstName")),
                        new M2MPropertyMapping("salaryBand",
                                M2MExpressionParser.parse(
                                        "if($src.salary < 50000, |'Entry', |if($src.salary < 100000, |'Mid', |'Senior'))"))
                )
        );
        
        M2MCompiler compiler = new M2MCompiler(rawPersonMapping, "t0");
        RelationNode plan = compiler.compile(mapping);
        
        String sql = generator.generate(plan);
        System.out.println("Generated SQL: " + sql);
        
        // Execute and verify
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
