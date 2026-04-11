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
}
