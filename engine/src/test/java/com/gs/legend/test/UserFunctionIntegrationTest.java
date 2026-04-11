package com.gs.legend.test;

import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.parser.PureParseException;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.plan.SingleExecutionPlan;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for user-defined Pure functions.
 *
 * <p>Tests the full pipeline: parse → model-build → compile (AST substitution,
 * type check, overload resolution) → plan generate → execute against DuckDB.
 *
 * <p>Happy-path tests execute SQL against DuckDB and assert on actual values.
 * Error-case tests verify compile-time exceptions.
 *
 * <p>Test data: John Smith (30), Jane Smith (28), Bob Jones (45).
 */
@DisplayName("User Function Integration Tests")
class UserFunctionIntegrationTest {

    // ==================== Model Source ====================

    private static final String BASE_MODEL = """
            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }

            Database store::PersonDatabase
            (
                Table T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )
            )

            Mapping model::PersonMapping
            (
                model::Person: Relational
                {
                    ~mainTable [PersonDatabase] T_PERSON
                    firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                    lastName: [PersonDatabase] T_PERSON.LAST_NAME,
                    age: [PersonDatabase] T_PERSON.AGE_VAL
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
                mappings: [ model::PersonMapping ];
                connections: [ store::PersonDatabase: [ environment: store::TestConnection ] ];
            }
            """;

    // ==================== Shared DuckDB Connection ====================

    private static Connection connection;
    private static final QueryService queryService = new QueryService();

    @BeforeAll
    static void initDb() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )""");
            stmt.execute("INSERT INTO T_PERSON VALUES (1, 'John', 'Smith', 30)");
            stmt.execute("INSERT INTO T_PERSON VALUES (2, 'Jane', 'Smith', 28)");
            stmt.execute("INSERT INTO T_PERSON VALUES (3, 'Bob', 'Jones', 45)");
            stmt.execute("""
                CREATE TABLE T_EMPLOYEE (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER,
                    DEPT VARCHAR(100)
                )""");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (10, 'Alice', 'Dev', 32, 'Engineering')");
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (11, 'Charlie', 'Mgr', 40, 'Sales')");
        }
    }

    @AfterAll
    static void closeDb() throws SQLException {
        if (connection != null && !connection.isClosed()) connection.close();
    }

    // ==================== Helpers ====================

    private static String modelWith(String functions) {
        return BASE_MODEL + "\n" + functions;
    }

    private static SingleExecutionPlan plan(String pureSource, String query) {
        return PlanGenerator.generate(pureSource, query, "test::TestRuntime");
    }

    private static ExecutionResult exec(String pureSource, String query) throws SQLException {
        return queryService.execute(pureSource, query, "test::TestRuntime", connection);
    }

    /** Extracts column values as a list from a single-column result. */
    private static <T> List<T> column(ExecutionResult result, int colIdx, Class<T> type) {
        return result.rows().stream()
                .map(r -> type.cast(r.get(colIdx)))
                .toList();
    }

    /** Extracts integer column values (handles Long from DuckDB). */
    private static List<Integer> intColumn(ExecutionResult result, int colIdx) {
        return result.rows().stream()
                .map(r -> ((Number) r.get(colIdx)).intValue())
                .toList();
    }

    // ==================== Basic Function Calls ====================

    @Nested
    @DisplayName("Basic Function Calls")
    class BasicCalls {

        @Test
        @DisplayName("Zero-param function returning literal")
        void testZeroParamFunction() throws SQLException {
            String model = modelWith("""
                    function test::greeting():String[1]
                    {
                        'hello'
                    }
                    """);
            // greeting() == 'hello' is always true → all 3 rows returned
            var result = exec(model,
                    "|model::Person.all()->project([x|$x.firstName], ['name'])->filter(x|test::greeting() == 'hello')");
            assertEquals(3, result.rowCount());
        }

        @Test
        @DisplayName("Single-param function — standalone call")
        void testSingleParamStandaloneCall() throws SQLException {
            String model = modelWith("""
                    function test::doubleAge(a: Integer[1]):Integer[1]
                    {
                        $a * 2
                    }
                    """);
            // John=60, Jane=56, Bob=90
            var result = exec(model,
                    "|model::Person.all()->project([x|test::doubleAge($x.age)], ['doubledAge'])");
            assertEquals(3, result.rowCount());
            var values = intColumn(result, 0);
            assertTrue(values.contains(60), "John 30*2=60: " + values);
            assertTrue(values.contains(56), "Jane 28*2=56: " + values);
            assertTrue(values.contains(90), "Bob 45*2=90: " + values);
        }

        @Test
        @DisplayName("Multi-param function")
        void testMultiParamFunction() throws SQLException {
            String model = modelWith("""
                    function test::fullName(first: String[1], last: String[1]):String[1]
                    {
                        $first + ' ' + $last
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([x|test::fullName($x.firstName, $x.lastName)], ['full'])");
            var values = column(result, 0, String.class);
            assertTrue(values.contains("John Smith"), "Expected 'John Smith': " + values);
            assertTrue(values.contains("Bob Jones"), "Expected 'Bob Jones': " + values);
        }
    }

    // ==================== Multi-Statement Bodies ====================

    @Nested
    @DisplayName("Multi-Statement Bodies (Let Chains)")
    class LetChains {

        @Test
        @DisplayName("Let chain with intermediate computation")
        void testMultiStatementLetChain() throws SQLException {
            String model = modelWith("""
                    function test::compute(x: Integer[1]):Integer[1]
                    {
                        let doubled = $x * 2;
                        let result = $doubled + 10;
                        $result;
                    }
                    """);
            // compute(30)=70, compute(28)=66, compute(45)=100
            var result = exec(model,
                    "|model::Person.all()->project([x|test::compute($x.age)], ['computed'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(70), "30*2+10=70: " + values);
            assertTrue(values.contains(66), "28*2+10=66: " + values);
            assertTrue(values.contains(100), "45*2+10=100: " + values);
        }

        @Test
        @DisplayName("Let as last statement (value is the result)")
        void testLetAsLastStatement() throws SQLException {
            String model = modelWith("""
                    function test::lastLet(x: Integer[1]):Integer[1]
                    {
                        let result = $x + 100
                    }
                    """);
            // lastLet(30)=130, lastLet(28)=128, lastLet(45)=145
            var result = exec(model,
                    "|model::Person.all()->project([x|test::lastLet($x.age)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(130), "30+100=130: " + values);
            assertTrue(values.contains(145), "45+100=145: " + values);
        }
    }

    // ==================== Nested / Composition ====================

    @Nested
    @DisplayName("Function Composition")
    class Composition {

        @Test
        @DisplayName("Nested user function call — f() calls g()")
        void testNestedUserFunctionCall() throws SQLException {
            String model = modelWith("""
                    function test::inner(x: Integer[1]):Integer[1]
                    {
                        $x * 3
                    }

                    function test::outer(x: Integer[1]):Integer[1]
                    {
                        test::inner($x) + 1
                    }
                    """);
            // outer(30) = 30*3+1 = 91, outer(45) = 45*3+1 = 136
            var result = exec(model,
                    "|model::Person.all()->project([x|test::outer($x.age)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(91), "30*3+1=91: " + values);
            assertTrue(values.contains(136), "45*3+1=136: " + values);
        }

        @Test
        @DisplayName("User function calling built-in functions")
        void testFunctionCallingBuiltin() throws SQLException {
            String model = modelWith("""
                    function test::isAdult(age: Integer[1]):Boolean[1]
                    {
                        $age >= 18
                    }
                    """);
            // All 3 are >= 18 → all 3 returned
            var result = exec(model,
                    "|model::Person.all()->filter(x|test::isAdult($x.age))->project([x|$x.firstName], ['name'])");
            assertEquals(3, result.rowCount());
            var names = column(result, 0, String.class);
            assertTrue(names.contains("John"), "All adults: " + names);
        }

        @Test
        @DisplayName("Complex expression as argument")
        void testComplexArgExpression() throws SQLException {
            String model = modelWith("""
                    function test::check(val: Integer[1]):Boolean[1]
                    {
                        $val > 100
                    }
                    """);
            // check(30*3+10)=check(100)=false, check(28*3+10)=check(94)=false, check(45*3+10)=check(145)=true
            var result = exec(model,
                    "|model::Person.all()->filter(x|test::check($x.age * 3 + 10))->project([x|$x.firstName], ['name'])");
            assertEquals(1, result.rowCount());
            assertEquals("Bob", result.rows().get(0).get(0));
        }
    }

    // ==================== Lambda Shadowing & Substitution Safety ====================

    @Nested
    @DisplayName("Substitution Safety")
    class SubstitutionSafety {

        @Test
        @DisplayName("Lambda param shadows function param (capture-avoiding)")
        void testParamShadowedByLambda() throws SQLException {
            String model = modelWith("""
                    function test::filterAdults(threshold: Integer[1]):Boolean[1]
                    {
                        $threshold > 0
                    }
                    """);
            // All ages > 0 → all 3 returned
            var result = exec(model,
                    "|model::Person.all()->filter(x|test::filterAdults($x.age))->project([x|$x.firstName], ['name'])");
            assertEquals(3, result.rowCount());
        }

        @Test
        @DisplayName("Prefix-safe: param 'x' does not match 'xx' (AST, not textual)")
        void testPrefixSafeSubstitution() throws SQLException {
            String model = modelWith("""
                    function test::calc(x: Integer[1]):Integer[1]
                    {
                        let xx = $x + 1;
                        $xx * 2;
                    }
                    """);
            // calc(30) = (30+1)*2 = 62, calc(45) = (45+1)*2 = 92
            var result = exec(model,
                    "|model::Person.all()->project([p|test::calc($p.age)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(62), "(30+1)*2=62: " + values);
            assertTrue(values.contains(92), "(45+1)*2=92: " + values);
        }
    }

    // ==================== Realistic Use Case ====================

    @Nested
    @DisplayName("Realistic Use Case")
    class RealisticUseCase {

        @Test
        @DisplayName("Bounded score — apply scoring fn, clamp to [0, cap]")
        void testBoundedScore() throws SQLException {
            String model = modelWith("""
                    function risk::boundedScore(scoreFn: {Integer[1]->Integer[1]}[1], value: Integer[1], cap: Integer[1]):Integer[1]
                    {
                        let raw = $scoreFn->eval($value);
                        if($raw < 0, |0, |if($raw > $cap, |$cap, |$raw));
                    }
                    """);
            // Triple and clamp to [0, 100]: 30*3=90 (pass), 28*3=84 (pass), 45*3=135 (capped to 100)
            var result = exec(model,
                    "|model::Person.all()->project([p|risk::boundedScore({x|$x * 3}, $p.age, 100)], ['score'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(90), "30*3=90, under cap: " + values);
            assertTrue(values.contains(84), "28*3=84, under cap: " + values);
            assertTrue(values.contains(100), "45*3=135, capped to 100: " + values);
        }
    }

    // ==================== Higher-Order Functions (Function as Param) ====================

    @Nested
    @DisplayName("Higher-Order Functions")
    class HigherOrder {

        @Test
        @DisplayName("Lambda passed as function param — apply(fn, val)")
        void testHigherOrderLambdaParam() throws SQLException {
            String model = modelWith("""
                    function test::apply(f: {Integer[1]->Integer[1]}[1], x: Integer[1]):Integer[1]
                    {
                        $f->eval($x)
                    }
                    """);
            // apply({y|y*2}, 30)=60, apply({y|y*2}, 45)=90
            var result = exec(model,
                    "|model::Person.all()->project([p|test::apply({y|$y * 2}, $p.age)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(60), "30*2=60: " + values);
            assertTrue(values.contains(90), "45*2=90: " + values);
        }

        @Test
        @DisplayName("Lambda param arity validation — wrong arity throws")
        void testLambdaArityMismatch() {
            String model = modelWith("""
                    function test::apply(f: {Integer[1]->Integer[1]}[1], x: Integer[1]):Integer[1]
                    {
                        $f->eval($x)
                    }
                    """);
            // Pass a 2-param lambda where 1-param is expected
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([p|test::apply({a,b|$a + $b}, $p.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("lambda") && ex.getMessage().contains("1 param"),
                    "Should report lambda arity mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Higher-order with let chain in body")
        void testHigherOrderWithLetChain() throws SQLException {
            String model = modelWith("""
                    function test::applyAndAdd(f: {Integer[1]->Integer[1]}[1], x: Integer[1], bonus: Integer[1]):Integer[1]
                    {
                        let result = $f->eval($x);
                        $result + $bonus;
                    }
                    """);
            // applyAndAdd({y|y*2}, 30, 100) = 60+100 = 160
            var result = exec(model,
                    "|model::Person.all()->project([p|test::applyAndAdd({y|$y * 2}, $p.age, 100)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(160), "30*2+100=160: " + values);
            assertTrue(values.contains(190), "45*2+100=190: " + values);
        }

        @Test
        @DisplayName("Higher-order composed with regular params")
        void testHigherOrderMixedParams() throws SQLException {
            String model = modelWith("""
                    function test::applyWithOffset(f: {Integer[1]->Integer[1]}[1], x: Integer[1], offset: Integer[1]):Integer[1]
                    {
                        $f->eval($x) + $offset
                    }
                    """);
            // 30*3+100=190, 28*3+100=184, 45*3+100=235
            var result = exec(model,
                    "|model::Person.all()->project([p|test::applyWithOffset({y|$y * 3}, $p.age, 100)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(190), "30*3+100=190: " + values);
            assertTrue(values.contains(235), "45*3+100=235: " + values);
        }

        @Test
        @DisplayName("Nested higher-order — user fn calls another user fn with lambda forwarding")
        void testNestedHigherOrder() throws SQLException {
            String model = modelWith("""
                    function test::apply(f: {Integer[1]->Integer[1]}[1], x: Integer[1]):Integer[1]
                    {
                        $f->eval($x)
                    }

                    function test::doubleApply(x: Integer[1]):Integer[1]
                    {
                        test::apply({y|$y * 2}, $x)
                    }
                    """);
            // doubleApply(30)=60, doubleApply(45)=90
            var result = exec(model,
                    "|model::Person.all()->project([p|test::doubleApply($p.age)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(60), "30*2=60: " + values);
            assertTrue(values.contains(90), "45*2=90: " + values);
        }
    }

    // ==================== Overloading ====================

    @Nested
    @DisplayName("Overloading")
    class Overloading {

        @Test
        @DisplayName("Overload by arity — 1-arg vs 2-arg")
        void testOverloadByArity() throws SQLException {
            String model = modelWith("""
                    function test::fmt(val: String[1]):String[1]
                    {
                        $val + '!'
                    }

                    function test::fmt(val: String[1], suffix: String[1]):String[1]
                    {
                        $val + $suffix
                    }
                    """);
            // 1-arg: "John" + "!" = "John!"
            var r1 = exec(model,
                    "|model::Person.all()->project([x|test::fmt($x.firstName)], ['f'])");
            var v1 = column(r1, 0, String.class);
            assertTrue(v1.contains("John!"), "1-arg overload: " + v1);

            // 2-arg: "John" + "..." = "John..."
            var r2 = exec(model,
                    "|model::Person.all()->project([x|test::fmt($x.firstName, '...')], ['f'])");
            var v2 = column(r2, 0, String.class);
            assertTrue(v2.contains("John..."), "2-arg overload: " + v2);
        }
    }

    // ==================== Error Cases ====================

    @Nested
    @DisplayName("Error Cases")
    class ErrorCases {

        @Test
        @DisplayName("Arity mismatch throws clear error")
        void testArityMismatchThrows() {
            String model = modelWith("""
                    function test::oneArg(x: Integer[1]):Integer[1]
                    {
                        $x + 1
                    }
                    """);
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::oneArg($x.age, $x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("No overload"), "Should report arity mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Recursion guard throws at depth > 32")
        void testRecursionGuardThrows() {
            String model = modelWith("""
                    function test::recurse(x: Integer[1]):Integer[1]
                    {
                        test::recurse($x)
                    }
                    """);
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::recurse($x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("inline depth"),
                    "Should report recursion: " + ex.getMessage());
        }

        @Test
        @DisplayName("Return type mismatch throws")
        void testReturnTypeMismatchThrows() {
            String model = modelWith("""
                    function test::wrongReturn(x: Integer[1]):Boolean[1]
                    {
                        $x + 1
                    }
                    """);
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::wrongReturn($x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("return type") || ex.getMessage().contains("declares return type"),
                    "Should report return type mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Empty function body rejected at parse time")
        void testEmptyBodyThrows() {
            assertThrows(PureParseException.class, () ->
                    com.gs.legend.parser.PureParser.parseCodeBlock(""));
            assertThrows(PureParseException.class, () ->
                    com.gs.legend.parser.PureParser.parseCodeBlock("   "));
        }

        @Test
        @DisplayName("Lambda return type mismatch in higher-order param throws")
        void testLambdaReturnTypeMismatch() {
            String model = modelWith("""
                    function test::apply(f: {Integer[1]->Boolean[1]}[1], x: Integer[1]):Boolean[1]
                    {
                        $f->eval($x)
                    }
                    """);
            // Lambda returns Integer (x*2), but FunctionType expects Boolean
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([p|test::apply({y|$y * 2}, $p.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("return type") || ex.getMessage().contains("Lambda return"),
                    "Should report lambda return type mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Parameter type mismatch throws")
        void testParamTypeMismatchThrows() {
            String model = modelWith("""
                    function test::needsString(s: String[1]):String[1]
                    {
                        $s + '!'
                    }
                    """);
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::needsString($x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("expects") && ex.getMessage().contains("String"),
                    "Should report type mismatch: " + ex.getMessage());
        }
    }

    // ==================== Type Compatibility ====================

    @Nested
    @DisplayName("Type Compatibility")
    class TypeCompatibility {

        @Test
        @DisplayName("Integer arg accepted for Number param (numeric hierarchy)")
        void testNumericHierarchy() throws SQLException {
            String model = modelWith("""
                    function test::doubleNum(n: Number[1]):Number[1]
                    {
                        $n * 2
                    }
                    """);
            // 30*2=60, 28*2=56, 45*2=90
            var result = exec(model,
                    "|model::Person.all()->project([p|test::doubleNum($p.age)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(60), "30*2=60: " + values);
            assertTrue(values.contains(90), "45*2=90: " + values);
        }

        @Test
        @DisplayName("Overload resolved by type — Integer vs String")
        void testOverloadByType() throws SQLException {
            String model = modelWith("""
                    function test::show(x: Integer[1]):Integer[1]
                    {
                        $x * 10
                    }

                    function test::show(x: String[1]):String[1]
                    {
                        $x + '!'
                    }
                    """);
            // Integer overload: 30*10=300
            var ri = exec(model,
                    "|model::Person.all()->project([p|test::show($p.age)], ['val'])");
            var iv = intColumn(ri, 0);
            assertTrue(iv.contains(300), "30*10=300: " + iv);

            // String overload: "John" + "!" = "John!"
            var rs = exec(model,
                    "|model::Person.all()->project([p|test::show($p.firstName)], ['val'])");
            var sv = column(rs, 0, String.class);
            assertTrue(sv.contains("John!"), "String overload: " + sv);
        }

        @Test
        @DisplayName("Ambiguous overload throws")
        void testAmbiguousOverloadThrows() {
            String model = modelWith("""
                    function test::dup(x: Integer[1]):Integer[1]
                    {
                        $x + 1
                    }

                    function test::dup(x: Integer[1]):Integer[1]
                    {
                        $x + 2
                    }
                    """);
            var ex = assertThrows(PureCompileException.class, () ->
                    plan(model, "|model::Person.all()->project([p|test::dup($p.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("Ambiguous"),
                    "Should report ambiguous overload: " + ex.getMessage());
        }
    }

    // ==================== Feature Composition ====================

    @Nested
    @DisplayName("Feature Composition — multiple features interacting")
    class FeatureComposition {

        @Test
        @DisplayName("Nested call + let chain + built-in + filter")
        void testNestedLetChainBuiltinFilter() throws SQLException {
            String model = modelWith("""
                    function test::ageCategory(age: Integer[1]):Integer[1]
                    {
                        let base = $age * 10;
                        $base + 5;
                    }

                    function test::isHighCategory(age: Integer[1]):Boolean[1]
                    {
                        test::ageCategory($age) > 200
                    }
                    """);
            // ageCategory(30)=305>200✓, ageCategory(28)=285>200✓, ageCategory(45)=455>200✓
            var result = exec(model,
                    "|model::Person.all()->filter(x|test::isHighCategory($x.age))->project([x|$x.firstName], ['name'])");
            assertEquals(3, result.rowCount(), "All categories > 200");
        }

        @Test
        @DisplayName("User function in both filter and project")
        void testFunctionInFilterAndProject() throws SQLException {
            String model = modelWith("""
                    function test::isOldEnough(age: Integer[1]):Boolean[1]
                    {
                        $age >= 21
                    }

                    function test::label(name: String[1]):String[1]
                    {
                        $name + ' [verified]'
                    }
                    """);
            // John(30)✓, Jane(28)✓, Bob(45)✓ — all >= 21
            var result = exec(model,
                    "|model::Person.all()->filter(x|test::isOldEnough($x.age))->project([x|test::label($x.firstName)], ['label'])");
            var labels = column(result, 0, String.class);
            assertTrue(labels.contains("John [verified]"), "Label: " + labels);
            assertTrue(labels.contains("Bob [verified]"), "Label: " + labels);
        }

        @Test
        @DisplayName("Overloaded function + nested call")
        void testOverloadPlusNesting() throws SQLException {
            String model = modelWith("""
                    function test::transform(x: Integer[1]):Integer[1]
                    {
                        $x * 2
                    }

                    function test::transform(x: Integer[1], y: Integer[1]):Integer[1]
                    {
                        test::transform($x) + $y
                    }
                    """);
            // transform(30, 100) = 30*2 + 100 = 160, transform(45, 100) = 45*2 + 100 = 190
            var result = exec(model,
                    "|model::Person.all()->project([x|test::transform($x.age, 100)], ['val'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(160), "30*2+100=160: " + values);
            assertTrue(values.contains(190), "45*2+100=190: " + values);
        }

        @Test
        @DisplayName("User function with multiple column references")
        void testFunctionWithMultipleColumns() throws SQLException {
            String model = modelWith("""
                    function test::nameAndAge(name: String[1], age: Integer[1]):String[1]
                    {
                        $name + ' is ' + toString($age)
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([x|test::nameAndAge($x.firstName, $x.age)], ['desc'])");
            var values = column(result, 0, String.class);
            assertTrue(values.contains("John is 30"), "Multi-col: " + values);
            assertTrue(values.contains("Bob is 45"), "Multi-col: " + values);
        }

        @Test
        @DisplayName("Let chain + prefix safety + nested call composed")
        void testFullComposition() throws SQLException {
            String model = modelWith("""
                    function test::step1(val: Integer[1]):Integer[1]
                    {
                        $val + 1
                    }

                    function test::pipeline(x: Integer[1]):Integer[1]
                    {
                        let a = test::step1($x);
                        let aa = $a * 2;
                        $aa + $a;
                    }
                    """);
            // pipeline(30): a=31, aa=62, result=62+31=93
            // pipeline(45): a=46, aa=92, result=92+46=138
            var result = exec(model,
                    "|model::Person.all()->project([p|test::pipeline($p.age)], ['result'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(93), "31*2+31=93: " + values);
            assertTrue(values.contains(138), "46*2+46=138: " + values);
        }
    }

    // ==================== Query-Returning Functions ====================

    @Nested
    @DisplayName("Query-Returning Functions — functions that return class queries or relations")
    class QueryReturning {

        @Test
        @DisplayName("Zero-param class query function, chain project at call site")
        void testZeroParamClassQuery() throws SQLException {
            String model = modelWith("""
                    function test::allPersons():Person[*]
                    {
                        model::Person.all()
                    }
                    """);
            var result = exec(model,
                    "|test::allPersons()->project([p|$p.firstName], ['name'])");
            assertEquals(3, result.rowCount(), "All 3 persons");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("John"), "Names: " + names);
            assertTrue(names.contains("Bob"), "Names: " + names);
        }

        @Test
        @DisplayName("Parameterized class query, chain project at call site")
        void testParameterizedClassQuery() throws SQLException {
            String model = modelWith("""
                    function test::olderThan(minAge: Integer[1]):Person[*]
                    {
                        model::Person.all()->filter(p|$p.age > $minAge)
                    }
                    """);
            var result = exec(model,
                    "|test::olderThan(30)->project([p|$p.firstName], ['name'])");
            assertEquals(1, result.rowCount(), "Only Bob(45) > 30");
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Class query + chain filter at call site")
        void testClassQueryChainFilter() throws SQLException {
            String model = modelWith("""
                    function test::smiths():Person[*]
                    {
                        model::Person.all()->filter(p|$p.lastName == 'Smith')
                    }
                    """);
            // Chain another filter on top: age > 29 → John(30) only
            var result = exec(model,
                    "|test::smiths()->filter(x|$x.age > 29)->project([p|$p.firstName], ['name'])");
            assertEquals(1, result.rowCount(), "Only John Smith(30) > 29");
            assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Class query + chain sort at call site")
        void testClassQueryChainSort() throws SQLException {
            String model = modelWith("""
                    function test::allPersons():Person[*]
                    {
                        model::Person.all()
                    }
                    """);
            var result = exec(model,
                    "|test::allPersons()->project([p|$p.firstName, p|$p.age], ['name', 'age'])->sort('age', SortDirection.ASC)");
            assertEquals(3, result.rowCount());
            var ages = intColumn(result, 1);
            assertEquals(List.of(28, 30, 45), ages, "Sorted ascending by age");
        }

        @Test
        @DisplayName("Multi-param class query: name filter + age threshold")
        void testMultiParamClassQuery() throws SQLException {
            String model = modelWith("""
                    function test::nameAndAge(surname: String[1], minAge: Integer[1]):Person[*]
                    {
                        model::Person.all()->filter(p|$p.lastName == $surname && $p.age >= $minAge)
                    }
                    """);
            var result = exec(model,
                    "|test::nameAndAge('Smith', 30)->project([p|$p.firstName], ['name'])");
            assertEquals(1, result.rowCount(), "Only John Smith(30) >= 30");
            assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Relation-returning: table reference in body, chain filter at call site")
        void testRelationReturningTableRef() throws SQLException {
            String model = modelWith("""
                    function test::personRelation():Any[*]
                    {
                        #>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL])
                    }
                    """);
            var result = exec(model,
                    "|test::personRelation()->filter(x|$x.AGE_VAL > 30)");
            assertEquals(1, result.rowCount(), "Only Bob(45) > 30");
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Parameterized relation-returning: threshold in body filter")
        void testParameterizedRelationReturning() throws SQLException {
            String model = modelWith("""
                    function test::personAbove(minAge: Integer[1]):Any[*]
                    {
                        #>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL])->filter(x|$x.AGE_VAL > $minAge)
                    }
                    """);
            var result = exec(model,
                    "|test::personAbove(28)");
            assertEquals(2, result.rowCount(), "John(30) and Bob(45) > 28");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("John"), "Names: " + names);
            assertTrue(names.contains("Bob"), "Names: " + names);
        }

        @Test
        @DisplayName("Class query as parameter: addLabel processes filtered input")
        void testClassQueryAsParameter() throws SQLException {
            String model = modelWith("""
                    function test::nameList(people: Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName, p|$p.age], ['name', 'age'])
                    }
                    """);
            // Pass filtered class query as argument
            var result = exec(model,
                    "|test::nameList(model::Person.all()->filter(p|$p.lastName == 'Smith'))");
            assertEquals(2, result.rowCount(), "2 Smiths");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("John"), "Names: " + names);
            assertTrue(names.contains("Jane"), "Names: " + names);
        }

        @Test
        @DisplayName("Query-returning fn calls another query-returning fn (nested inlining)")
        void testNestedQueryFunctions() throws SQLException {
            String model = modelWith("""
                    function test::adults():Person[*]
                    {
                        model::Person.all()->filter(p|$p.age >= 28)
                    }
                    function test::adultSmiths():Person[*]
                    {
                        test::adults()->filter(p|$p.lastName == 'Smith')
                    }
                    """);
            var result = exec(model,
                    "|test::adultSmiths()->project([p|$p.firstName, p|$p.age], ['name', 'age'])");
            assertEquals(2, result.rowCount(), "John(30) and Jane(28)");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("John"), "Names: " + names);
            assertTrue(names.contains("Jane"), "Names: " + names);
        }

        @Test
        @DisplayName("Scalar fn applied to query-returning fn result in project")
        void testScalarOnQueryReturning() throws SQLException {
            String model = modelWith("""
                    function test::adults():Person[*]
                    {
                        model::Person.all()->filter(p|$p.age >= 28)
                    }
                    function test::doubleAge(n: Integer[1]):Integer[1]
                    {
                        $n * 2
                    }
                    """);
            // Query fn for source, scalar fn in projection
            var result = exec(model,
                    "|test::adults()->project([p|test::doubleAge($p.age)], ['doubled'])");
            var values = intColumn(result, 0);
            assertTrue(values.contains(60), "30*2=60: " + values);
            assertTrue(values.contains(56), "28*2=56: " + values);
            assertTrue(values.contains(90), "45*2=90: " + values);
        }

        @Test
        @DisplayName("Type check: wrong class query for typed param rejects at compile time")
        void testWrongClassParamRejected() {
            // Declare param as Person[*], but we'll need a different class to pass
            // Since we only have Person in the model, declare param as String[*]
            // and pass Person.all() → should fail: "Person" vs "String"
            String model = modelWith("""
                    function test::badParam(names: String[*]):Any[*]
                    {
                        $names
                    }
                    """);
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::badParam(model::Person.all())"));
            assertTrue(ex.getMessage().contains("expects String but got Person"),
                    "Should report type mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Type check: class query matches declared class param")
        void testClassParamTypeChecked() throws SQLException {
            String model = modelWith("""
                    function test::getNames(people: Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName], ['name'])
                    }
                    """);
            // Correct type: Person.all() → Person[*] matches Person[*] param
            var result = exec(model, "|test::getNames(model::Person.all())");
            assertEquals(3, result.rowCount());
        }

        @Test
        @DisplayName("Type check: subtype accepted for supertype param (Employee extends Person)")
        void testSubtypeAccepted() throws SQLException {
            // Full standalone model: Employee extends Person, own table, own mapping
            String model = """
                    Class model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];
                        age: Integer[1];
                    }
                    Class model::Employee extends model::Person
                    {
                        dept: String[1];
                    }
                    Database store::PersonDatabase
                    (
                        Table T_EMPLOYEE (
                            ID INTEGER PRIMARY KEY,
                            FIRST_NAME VARCHAR(100),
                            LAST_NAME VARCHAR(100),
                            AGE_VAL INTEGER,
                            DEPT VARCHAR(100)
                        )
                    )
                    Mapping model::EmpMapping
                    (
                        model::Employee: Relational
                        {
                            ~mainTable [PersonDatabase] T_EMPLOYEE
                            firstName: [PersonDatabase] T_EMPLOYEE.FIRST_NAME,
                            lastName: [PersonDatabase] T_EMPLOYEE.LAST_NAME,
                            age: [PersonDatabase] T_EMPLOYEE.AGE_VAL,
                            dept: [PersonDatabase] T_EMPLOYEE.DEPT
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
                        mappings: [ model::EmpMapping ];
                        connections: [ store::PersonDatabase: [ environment: store::TestConnection ] ];
                    }
                    function test::getNames(people: Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName], ['name'])
                    }
                    """;
            // Employee extends Person, so Employee.all() passes Person[*] type check
            // Then runs E2E: Employee.all() → project firstName → real SQL → real DuckDB data
            var result = exec(model, "|test::getNames(model::Employee.all())");
            assertEquals(2, result.rowCount(), "Alice and Charlie");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("Alice"), "Names: " + names);
            assertTrue(names.contains("Charlie"), "Names: " + names);
        }

        @Test
        @DisplayName("Type check: unrelated class rejected for typed param")
        void testUnrelatedClassRejected() {
            String model = BASE_MODEL + """
                    Class model::Firm
                    {
                        legalName: String[1];
                    }
                    function test::getNames(people: Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName], ['name'])
                    }
                    """;
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::getNames(model::Firm.all())"));
            assertTrue(ex.getMessage().contains("expects Person but got Firm"),
                    "Should reject unrelated class: " + ex.getMessage());
        }

        @Test
        @DisplayName("Return type check: declares Person but body returns Integer rejects")
        void testReturnTypeMismatchRejected() {
            String model = modelWith("""
                    function test::badReturn():Person[*]
                    {
                        1 + 2
                    }
                    """);
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::badReturn()"));
            assertTrue(ex.getMessage().contains("declares return type Person but body returns Integer"),
                    "Should report return type mismatch: " + ex.getMessage());
        }

        // ===== Typed Relation<(schema)> param + return tests =====

        @Test
        @DisplayName("Typed Relation: superset param accepted, typed return validated")
        void testTypedRelationParamSuperset() throws SQLException {
            // Param declares (FIRST_NAME:String), return declares same.
            // Caller passes superset (FIRST_NAME + AGE_VAL) — param superset OK.
            // Body filter preserves all columns — body schema ⊇ return schema (covariant OK).
            String model = modelWith("""
                    function test::justNames(data: Relation<(FIRST_NAME:String)>[1]):Relation<(FIRST_NAME:String)>[1]
                    {
                        $data->filter(x|$x.FIRST_NAME == 'John')
                    }
                    """);
            var result = exec(model,
                    "|test::justNames(#>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL]))");
            assertEquals(1, result.rowCount(), "Only John");
            assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Typed Relation: missing column rejected at param boundary")
        void testTypedRelationParamMissingColumn() {
            // Declares FIRST_NAME in schema, caller passes only AGE_VAL → param check fails
            String model = modelWith("""
                    function test::needsName(data: Relation<(FIRST_NAME:String)>[1]):Relation<(FIRST_NAME:String)>[1]
                    {
                        $data->filter(x|$x.FIRST_NAME == 'John')
                    }
                    """);
            var ex = assertThrows(Exception.class, () ->
                    exec(model,
                            "|test::needsName(#>{store::PersonDatabase.T_PERSON}#->select(~[AGE_VAL]))"));
            assertTrue(ex.getMessage().contains("schema mismatch") && ex.getMessage().contains("missing column")
                            && ex.getMessage().contains("FIRST_NAME"),
                    "Should report missing column at boundary: " + ex.getMessage());
        }

        @Test
        @DisplayName("Typed Relation: column type hierarchy — Integer satisfies Number")
        void testTypedRelationParamColumnTypeHierarchy() throws SQLException {
            // Declares AGE_VAL as Number — actual is Integer (subtype). Covariant return same.
            String model = modelWith("""
                    function test::ageFilter(data: Relation<(AGE_VAL:Number)>[1]):Relation<(AGE_VAL:Number)>[1]
                    {
                        $data->filter(x|$x.AGE_VAL > 30)
                    }
                    """);
            var result = exec(model,
                    "|test::ageFilter(#>{store::PersonDatabase.T_PERSON}#->select(~[AGE_VAL]))");
            assertEquals(1, result.rowCount(), "Only Bob(45)");
        }

        @Test
        @DisplayName("Typed Relation: column type mismatch rejected at param boundary")
        void testTypedRelationParamColumnTypeMismatch() {
            // Declares AGE_VAL as String — actual is Integer → type mismatch at param
            String model = modelWith("""
                    function test::badType(data: Relation<(AGE_VAL:String)>[1]):Relation<(AGE_VAL:String)>[1]
                    {
                        $data->filter(x|$x.AGE_VAL == 'foo')
                    }
                    """);
            var ex = assertThrows(Exception.class, () ->
                    exec(model,
                            "|test::badType(#>{store::PersonDatabase.T_PERSON}#->select(~[AGE_VAL]))"));
            assertTrue(ex.getMessage().contains("schema mismatch") && ex.getMessage().contains("AGE_VAL")
                            && ex.getMessage().contains("expects String") && ex.getMessage().contains("got Integer"),
                    "Should report column type mismatch at boundary: " + ex.getMessage());
        }

        @Test
        @DisplayName("Bare Relation param (no schema): still works via structural inlining")
        void testBareRelationParamStillWorks() throws SQLException {
            // Bare Any[*] without schema — no boundary check, structural validation via inlining
            String model = modelWith("""
                    function test::bigAges(data: Any[*]):Any[*]
                    {
                        $data->filter(x|$x.AGE_VAL > 30)
                    }
                    """);
            var result = exec(model,
                    "|test::bigAges(#>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL]))->filter(y|$y.FIRST_NAME == 'Bob')");
            assertEquals(1, result.rowCount(), "Only Bob(45) > 30");
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Typed Relation: multi-column schema with typed return, full E2E")
        void testTypedRelationParamMultiColumn() throws SQLException {
            // Both param and return declare full schema — covariant return check passes
            String model = modelWith("""
                    function test::nameAndAge(data: Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]):Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]
                    {
                        $data->filter(x|$x.AGE_VAL > 30)
                    }
                    """);
            var result = exec(model,
                    "|test::nameAndAge(#>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL]))");
            assertEquals(1, result.rowCount(), "Only Bob(45)");
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Covariant return: param has superset, return declares subset — body preserves all")
        void testCovariantReturnSubsetDeclared() throws SQLException {
            // Param declares (FIRST_NAME, AGE_VAL), return declares only (FIRST_NAME).
            // filter preserves all columns → body schema ⊇ return schema → covariant OK.
            // Caller chains on AGE_VAL (available via inlining even though not in return decl).
            String model = modelWith("""
                    function test::filterAge(data: Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]):Relation<(FIRST_NAME:String)>[1]
                    {
                        $data->filter(x|$x.AGE_VAL > 30)
                    }
                    """);
            var result = exec(model,
                    "|test::filterAge(#>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL]))");
            assertEquals(1, result.rowCount(), "Only Bob(45)");
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        // ===== Multiplicity validation tests =====

        @Test
        @DisplayName("Multiplicity: [1] param accepts scalar [1] argument")
        void testMultiplicityOneAcceptsOne() throws SQLException {
            // Integer[1] param with a scalar literal → OK
            String model = modelWith("""
                    function test::addOne(x: Integer[1]):Integer[1]
                    {
                        $x + 1
                    }
                    """);
            var result = exec(model, "|test::addOne(5)");
            assertEquals(1, result.rowCount());
            assertEquals(6, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        @DisplayName("Multiplicity: [0..1] param accepts scalar [1] argument")
        void testMultiplicityOptionalAcceptsOne() throws SQLException {
            // Integer[0..1] param — passing [1] is valid (narrower range fits)
            String model = modelWith("""
                    function test::maybeAdd(x: Integer[0..1]):Integer[1]
                    {
                        $x + 1
                    }
                    """);
            var result = exec(model, "|test::maybeAdd(5)");
            assertEquals(1, result.rowCount());
            assertEquals(6, ((Number) result.rows().get(0).get(0)).intValue());
        }
    }
}
