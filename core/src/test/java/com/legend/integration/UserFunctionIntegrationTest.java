package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import com.legend.error.LegendCompileException;
import com.legend.plan.QueryPlan;
import com.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.Connection;
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
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class UserFunctionIntegrationTest {

    // ==================== Model Source ====================

    private static final String BASE_MODEL = """
            ###Pure
            import store::*;

            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }

            ###Relational
            Database store::PersonDatabase
            (
                Table T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )
            )

            ###Mapping
            import store::*;
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

            ###Connection
            import store::*;
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }

            ###Runtime
            import store::*;
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
        connection = LaneFloor.backend().open();
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
        // BASE_MODEL ends inside ###Runtime — appended domain elements must
        // reopen the default section like a real Legend document does
        return BASE_MODEL + "\n###Pure\n" + functions;
    }

    private static QueryPlan plan(String pureSource, String query) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(pureSource), query).plan("test::TestRuntime");
    }

    private static Lean.Result exec(String pureSource, String query) throws SQLException {
        return Lean.run(pureSource, query, "test::TestRuntime", connection);
    }

    /** Extracts column values as a list from a single-column result. */
    private static <T> List<T> column(Lean.Result result, int colIdx, Class<T> type) {
        return result.rows().stream()
                .map(r -> type.cast(r.get(colIdx)))
                .toList();
    }

    /** Extracts integer column values (handles Long from DuckDB). */
    private static List<Integer> intColumn(Lean.Result result, int colIdx) {
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
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([p|test::apply({a,b|$a + $b}, $p.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("lambda")
                            && (ex.getMessage().contains("1 param") || ex.getMessage().contains("expects 1")),
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
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::oneArg($x.age, $x.age)], ['val'])"));
            assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("no overload"), "Should report arity mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Recursion guard throws on self-recursive user functions")
        void testRecursionGuardThrows() {
            String model = modelWith("""
                    function test::recurse(x: Integer[1]):Integer[1]
                    {
                        test::recurse($x)
                    }
                    """);
            // CORE contract (UserCallInlinerTest.recursionIsLoud, the
            // relationalMapper leg): a recursive call no longer throws at
            // ingest — it STANDS (host call frames consume standing calls)
            // and SQL-bound use fails LOUDLY at the resolver's
            // TypedUserCall wall, which names the recursion cycle.
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::recurse($x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("test::recurse/1")
                            && ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("cycl"),
                    "Should report recursion cycle involving test::recurse/1: " + ex.getMessage());
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
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::wrongReturn($x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("return type") || ex.getMessage().contains("declares return type"),
                    "Should report return type mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Empty function body rejected at parse time")
        void testEmptyBodyThrows() {
            assertThrows(com.legend.parser.ParseException.class, () ->
                    com.legend.testing.Own.spec(""));
            assertThrows(com.legend.parser.ParseException.class, () ->
                    com.legend.testing.Own.spec("   "));
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
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([p|test::apply({y|$y * 2}, $p.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("return type") || ex.getMessage().contains("Lambda return")
                            || ex.getMessage().contains("expected Boolean"),
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
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([x|test::needsString($x.age)], ['val'])"));
            assertTrue(ex.getMessage().contains("expect") && ex.getMessage().contains("String"),
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
            var ex = assertThrows(RuntimeException.class, () ->
                    plan(model, "|model::Person.all()->project([p|test::dup($p.age)], ['val'])"));
            assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("ambiguous"),
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
                    function test::allPersons():model::Person[*]
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
                    function test::olderThan(minAge: Integer[1]):model::Person[*]
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
                    function test::smiths():model::Person[*]
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
                    function test::allPersons():model::Person[*]
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
                    function test::nameAndAge(surname: String[1], minAge: Integer[1]):model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.lastName == $surname && ($p.age >= $minAge))
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
                    function test::personRelation():Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]
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
                    function test::personAbove(minAge: Integer[1]):Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]
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
                    function test::nameList(people: model::Person[*]):Any[*]
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
                    function test::adults():model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.age >= 28)
                    }
                    function test::adultSmiths():model::Person[*]
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
                    function test::adults():model::Person[*]
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
            // and pass model::Person.all() → should fail: "Person" vs "String"
            String model = modelWith("""
                    function test::badParam(names: String[*]):Any[*]
                    {
                        $names
                    }
                    """);
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::badParam(model::Person.all())"));
            assertTrue(ex.getMessage().contains("expects String but got Person")
                            || (ex.getMessage().contains("expected String") && ex.getMessage().contains("Person")),
                    "Should report type mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Type check: class query matches declared class param")
        void testClassParamTypeChecked() throws SQLException {
            String model = modelWith("""
                    function test::getNames(people: model::Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName], ['name'])
                    }
                    """);
            // Correct type: model::Person.all() → Person[*] matches Person[*] param
            var result = exec(model, "|test::getNames(model::Person.all())");
            assertEquals(3, result.rowCount());
        }

        @Test
        @DisplayName("Type check: subtype accepted for supertype param (Employee extends Person)")
        void testSubtypeAccepted() throws SQLException {
            // Full standalone model: Employee extends Person, own table, own mapping
            String model = """
                    ###Pure
                    import store::*;

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
                    ###Relational
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
                    ###Mapping
                    import store::*;
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
                    ###Connection
                    import store::*;
                    RelationalDatabaseConnection store::TestConnection
                    {
                        type: DuckDB;
                        specification: DuckDB { };
                        auth: Test;
                    }
                    ###Runtime
                    import store::*;
                    Runtime test::TestRuntime
                    {
                        mappings: [ model::EmpMapping ];
                        connections: [ store::PersonDatabase: [ environment: store::TestConnection ] ];
                    }
                    ###Pure
                    import store::*;
                    function test::getNames(people: model::Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName], ['name'])
                    }
                    """;
            // Employee extends Person, so model::Employee.all() passes Person[*] type check
            // Then runs E2E: model::Employee.all() → project firstName → real SQL → real DuckDB data
            var result = exec(model, "|test::getNames(model::Employee.all())");
            assertEquals(2, result.rowCount(), "Alice and Charlie");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("Alice"), "Names: " + names);
            assertTrue(names.contains("Charlie"), "Names: " + names);
        }

        @Test
        @DisplayName("Type check: unrelated class rejected for typed param")
        void testUnrelatedClassRejected() {
            String model = BASE_MODEL + "\n###Pure\n" + """
                    Class model::Firm
                    {
                        legalName: String[1];
                    }
                    function test::getNames(people: model::Person[*]):Any[*]
                    {
                        $people->project([p|$p.firstName], ['name'])
                    }
                    """;
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::getNames(model::Firm.all())"));
            assertTrue(ex.getMessage().contains("expects Person but got Firm")
                            || (ex.getMessage().contains("Person") && ex.getMessage().contains("got model::Firm")),
                    "Should reject unrelated class: " + ex.getMessage());
        }

        @Test
        @DisplayName("Return type check: declares Person but body returns Integer rejects")
        void testReturnTypeMismatchRejected() {
            String model = modelWith("""
                    function test::badReturn():model::Person[*]
                    {
                        1 + 2
                    }
                    """);
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::badReturn()"));
            assertTrue(ex.getMessage().contains("declares return type Person but body returns Integer"),
                    "Should report return type mismatch: " + ex.getMessage());
        }

        @Test
        @DisplayName("Return type check: declares Person but body returns wrong class — rejected")
        void testClassReturnWrongClassRejected() {
            String model = BASE_MODEL + "\n###Pure\n" + """
                    Class model::Firm
                    {
                        legalName: String[1];
                    }
                    function test::getPeople():model::Person[*]
                    {
                        model::Firm.all()
                    }
                    """;
            var ex = assertThrows(Exception.class, () ->
                    exec(model, "|test::getPeople()->project([p|$p.firstName], ['name'])"));
            assertTrue(ex.getMessage().contains("declares return type Person but body returns Firm"),
                    "Should reject wrong class return: " + ex.getMessage());
        }

        @Test
        @DisplayName("Return type check: Class[*] return — caller chains Person-specific props E2E")
        void testClassReturnTypeValidated() throws SQLException {
            // Declares Person[*], body returns model::Person.all()->filter(...)
            // Caller chains filter($p.lastName) + project($p.firstName, $p.age) — Person-specific
            String model = modelWith("""
                    function test::youngPeople():model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.age < 35)
                    }
                    """);
            var result = exec(model,
                    "|test::youngPeople()->filter(p|$p.lastName == 'Smith')->project([p|$p.firstName, p|$p.age], ['name', 'age'])");
            assertEquals(2, result.rowCount(), "John(30) and Jane(28) < 35 and Smith");
            var names = column(result, 0, String.class);
            assertTrue(names.contains("John"), "Names: " + names);
            assertTrue(names.contains("Jane"), "Names: " + names);
        }

        @Test
        @DisplayName("Baseline: graphFetch on model::Person.all() directly (no user function)")
        void testGraphFetchBaseline() throws SQLException {
            var result = exec(BASE_MODEL,
                    "|model::Person.all()->graphFetch(#{model::Person{firstName, age}}#)->serialize(#{model::Person{firstName, age}}#)");
            assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS, result.shape());
            String json = result.json();
            assertTrue(json.contains("John"), "JSON: " + json);
            assertTrue(json.contains("Jane"), "JSON: " + json);
        }

        @Test
        @DisplayName("Return type check: Class[*] return — graphFetch requires Person, full E2E")
        void testClassReturnGraphFetch() throws SQLException {
            String model = modelWith("""
                    function test::youngPeople():model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.age < 35)
                    }
                    """);
            var result = exec(model,
                    "|test::youngPeople()->graphFetch(#{model::Person{firstName, age}}#)->serialize(#{model::Person{firstName, age}}#)");
            assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS, result.shape());
            String json = result.json();
            assertTrue(json.contains("John"), "JSON: " + json);
            assertTrue(json.contains("Jane"), "JSON: " + json);
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
            assertTrue(ex.getMessage().contains("missing") && ex.getMessage().contains("column")
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
            assertTrue(ex.getMessage().contains("AGE_VAL")
                            && (ex.getMessage().contains("expects String") || ex.getMessage().contains("expected String"))
                            && ex.getMessage().contains("got Integer"),
                    "Should report column type mismatch at boundary: " + ex.getMessage());
        }

        @Test
        @DisplayName("Typed Relation param: works with explicit schema declaration")
        void testBareRelationParamStillWorks() throws SQLException {
            // Under decoupled check(PureFunction), bodies validate against declared signatures.
            // Property access on a relation param requires the declared type to be a typed
            // Relation<(...)> — Any[*] no longer structurally implies relation shape.
            String model = modelWith("""
                    function test::bigAges(data: Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]):Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]
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
            // Integer[0..1] param — passing [1] is valid (narrower range fits).
            // The body spells ->toOne(): real pure REJECTS [0..1] into
            // plus's [1] slot (MultiplicityMatch; multiplicity audit
            // slice 2 made the kernel strict).
            String model = modelWith("""
                    function test::maybeAdd(x: Integer[0..1]):Integer[1]
                    {
                        $x->toOne() + 1
                    }
                    """);
            var result = exec(model, "|test::maybeAdd(5)");
            assertEquals(1, result.rowCount());
            assertEquals(6, ((Number) result.rows().get(0).get(0)).intValue());
        }
    }

    // ==================== Edge-Case Probes ====================

    /**
     * Edge-case probes targeting patterns NOT covered by the existing test
     * classes. These pin behavior that the upcoming Resolver-time inlining
     * refactor must preserve.
     */
    @Nested
    @DisplayName("Edge-Case Probes")
    class EdgeCaseProbes {

        @Test
        @DisplayName("Probe 1 — same user-fn called twice in one query (alias uniqueness)")
        void testSameFnCalledTwice() throws SQLException {
            String model = modelWith("""
                    function test::adults(): model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.age >= 18)
                    }
                    """);
            // 3 adults in fixture × 2 (concatenate) = 6 rows. Project to tabular
            // so rowCount reflects rows (not the single JSON-aggregated row of
            // the graph result format).
            var result = exec(model,
                    "|test::adults()->concatenate(test::adults())->project([p|$p.firstName], ['name'])");
            assertEquals(6, result.rowCount());
        }

        @Test
        @DisplayName("Probe 2 — let-binds user-call result, chained relationally")
        void testLetBoundUserCallChained() throws SQLException {
            String model = modelWith("""
                    function test::adults(): model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.age >= 18)
                    }
                    function test::query(): Any[*]
                    {
                        let people = test::adults();
                        $people->project([p|$p.firstName], ['name']);
                    }
                    """);
            var result = exec(model, "|test::query()");
            assertEquals(3, result.rowCount());
            List<String> names = column(result, 0, String.class);
            assertTrue(names.contains("John"));
            assertTrue(names.contains("Jane"));
            assertTrue(names.contains("Bob"));
        }

        @Test
        @DisplayName("Probe 3 — user-call inside project lambda body")
        void testUserCallInsideProjectLambda() throws SQLException {
            String model = modelWith("""
                    function test::label(s: String[1]): String[1]
                    {
                        'Mr. ' + $s
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([p|test::label($p.firstName)], ['greeted'])");
            assertEquals(3, result.rowCount());
            List<String> labels = column(result, 0, String.class);
            assertTrue(labels.contains("Mr. John"));
            assertTrue(labels.contains("Mr. Jane"));
            assertTrue(labels.contains("Mr. Bob"));
        }

        @Test
        @DisplayName("Probe 4 — let-binding name shadows function param")
        void testLetShadowsFunctionParam() throws SQLException {
            String model = modelWith("""
                    function test::doubleThenInc(x: Integer[1]): Integer[1]
                    {
                        let x = $x * 2;
                        $x + 1;
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([p|test::doubleThenInc($p.age)], ['result'])");
            assertEquals(3, result.rowCount());
            List<Integer> values = intColumn(result, 0);
            // John 30 → 30*2+1 = 61, Jane 28 → 57, Bob 45 → 91
            assertTrue(values.contains(61), "John 30*2+1=61: " + values);
            assertTrue(values.contains(57), "Jane 28*2+1=57: " + values);
            assertTrue(values.contains(91), "Bob 45*2+1=91: " + values);
        }

        @Test
        @DisplayName("Probe 5 — function param in both branches of TypedIf")
        void testFunctionParamInBothIfBranches() throws SQLException {
            String model = modelWith("""
                    function test::safeAge(a: Integer[1]): Integer[1]
                    {
                        if($a < 0, |0, |$a)
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([p|test::safeAge($p.age)], ['adjusted'])");
            assertEquals(3, result.rowCount());
            List<Integer> values = intColumn(result, 0);
            // No negative ages → adjusted = age
            assertTrue(values.contains(30));
            assertTrue(values.contains(28));
            assertTrue(values.contains(45));
        }

        @Test
        @DisplayName("Probe 6 — f(g(x)) chained user-calls as scalar arg")
        void testChainedUserCallsScalarArg() throws SQLException {
            String model = modelWith("""
                    function test::dbl(x: Integer[1]): Integer[1]
                    {
                        $x * 2
                    }
                    function test::inc(x: Integer[1]): Integer[1]
                    {
                        $x + 1
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([p|test::inc(test::dbl($p.age))], ['result'])");
            assertEquals(3, result.rowCount());
            List<Integer> values = intColumn(result, 0);
            // John 30 → 30*2+1 = 61, Jane 28 → 57, Bob 45 → 91
            assertTrue(values.contains(61));
            assertTrue(values.contains(57));
            assertTrue(values.contains(91));
        }

        @Test
        @DisplayName("Probe 7 — nested TypedIf with multiple function params")
        void testNestedIfWithMultipleParams() throws SQLException {
            String model = modelWith("""
                    function test::clamp(v: Integer[1], lo: Integer[1], hi: Integer[1]): Integer[1]
                    {
                        if($v < $lo, |$lo, |if($v > $hi, |$hi, |$v))
                    }
                    """);
            var result = exec(model,
                    "|model::Person.all()->project([p|test::clamp($p.age, 20, 40)], ['clamped'])");
            assertEquals(3, result.rowCount());
            List<Integer> values = intColumn(result, 0);
            // John 30 stays, Jane 28 stays, Bob 45 → 40
            assertTrue(values.contains(30));
            assertTrue(values.contains(28));
            assertTrue(values.contains(40));
        }

        @Test
        @DisplayName("Probe 8 — relational + scalar user-call in same query")
        void testRelationalAndScalarUserCallSameQuery() throws SQLException {
            String model = modelWith("""
                    function test::adults(): model::Person[*]
                    {
                        model::Person.all()->filter(p|$p.age >= 18)
                    }
                    function test::targetSurname(): String[1]
                    {
                        'Smith'
                    }
                    """);
            // John & Jane are adult Smiths; Bob is Jones (excluded).
            // Project to tabular so rowCount reflects rows (not single JSON row).
            var result = exec(model,
                    "|test::adults()->filter(p|$p.lastName == test::targetSurname())->project([p|$p.firstName], ['name'])");
            assertEquals(2, result.rowCount());
            List<String> names = column(result, 0, String.class);
            assertTrue(names.contains("John"));
            assertTrue(names.contains("Jane"));
        }
    }

    // ==================== Functions generic over relations (docs/SEMANTICS_REGISTER.md S112) ====================

    /**
     * The static fold of a body expanded at its call (a function over a TDS, as the reference corpus's own are): a helper
     * the body calls is inlined with the arguments it was given, the static ones bound as facts. The second review of the
     * enumeration round, 2026-10-28: each a case the fold answered otherwise than the query it was folded from.
     */
    @Nested
    @DisplayName("The static fold of an expanded body")
    class FoldOfABody {

        private static final String PEOPLE = "|model::Person.all()->project([col(p|$p.firstName, 'first')])";

        private List<Object> column(Lean.Result result, int i) {
            return result.rows().stream().map(r -> r.get(i)).toList();
        }

        @Test
        @DisplayName("a function a helper was given and evaluates is written back as the call named it")
        void aFunctionPassedOnWhole() throws SQLException {
            // TypeToken, as EnumToken before it: test::up_String_1__String_1_ bound and used whole ($f->eval($s)) was left
            // a variable no binder held -- inside a helper, and as the element of a map unrolled
            String model = modelWith("""
                    function test::up(s: String[1]): String[1]
                    {
                        $s->toUpper()
                    }
                    function test::applyTo(s: String[1], f: Function<{String[1]->String[1]}>[1]): String[1]
                    {
                        $f->eval($s)
                    }
                    function test::upped(t: TabularDataSet[1]): TabularDataSet[1]
                    {
                        $t->extend([col(r:TDSRow[1]|test::applyTo('x', test::up_String_1__String_1_), 'one'),
                                    col(r:TDSRow[1]|[test::up_String_1__String_1_]->map(f|$f->eval('y'))->toOne(), 'each')])
                    }
                    """);
            var result = exec(model, PEOPLE + "->test::upped()->sort('first')");
            assertEquals(List.of("X", "X", "X"), column(result, 1));
            assertEquals(List.of("Y", "Y", "Y"), column(result, 2));
        }

        @Test
        @DisplayName("a helper's parameters are its own: an argument is read in the caller's scope, never captured")
        void aHelpersParametersAreItsOwn() throws SQLException {
            // swap(a, b) calling cat($b, $a) answered y-y in a body (cat's a read as swap's b already bound), and a row
            // the caller named b was read as the helper's b ('q')
            String model = modelWith("""
                    function test::cat(a: String[1], b: String[1]): String[1]
                    {
                        $a + '-' + $b
                    }
                    function test::swap(a: String[1], b: String[1]): String[1]
                    {
                        test::cat($b, $a)
                    }
                    function test::swapped(t: TabularDataSet[1]): TabularDataSet[1]
                    {
                        $t->extend([col(r:TDSRow[1]|test::swap('x', 'y'), 'swapped'),
                                    col(b:TDSRow[1]|test::cat($b.getString('first'), 'q'), 'named')])
                    }
                    """);
            assertEquals(List.of("y-x"), column(exec(model, "|test::swap('x', 'y')"), 0), "as the query answers it");
            var result = exec(model, PEOPLE + "->test::swapped()->sort('first')");
            assertEquals(List.of("y-x", "y-x", "y-x"), column(result, 1));
            assertEquals(List.of("Bob-q", "Jane-q", "John-q"), column(result, 2));
        }

        @Test
        @DisplayName("an enumeration's value against a string answers as the query does: the database compares the name")
        void anEnumerationAgainstAString() throws SQLException {
            // the fold compares two values of one enumeration; Stat.mean == 'mean' it leaves to the database, which
            // compares the value's name -- the fold had answered false where the same query answers true
            String model = modelWith("""
                    Enum test::Stat
                    {
                      mean, total
                    }
                    function test::said(t: TabularDataSet[1]): TabularDataSet[1]
                    {
                        $t->extend([col(r:TDSRow[1]|if(test::Stat.mean == 'mean', |'eq', |'ne'), 'mixed'),
                                    col(r:TDSRow[1]|if(test::Stat.mean == test::Stat.total, |'eq', |'ne'), 'two')])
                    }
                    """);
            var asked = column(exec(model, "|if(test::Stat.mean == 'mean', |'eq', |'ne')"), 0);
            var result = exec(model, PEOPLE + "->test::said()->sort('first')");
            assertEquals(List.of(asked.get(0), asked.get(0), asked.get(0)), column(result, 1), "as the query answers it");
            assertEquals(List.of("ne", "ne", "ne"), column(result, 2));
        }
    }

    /**
     * The user, 2026-10-26: a library of timeseries functions written in Pure over ANY table -- its columns, its sort
     * and its window parameters. legend-engine refuses type parameters in a user function; legend-lite expands a call
     * of one generic over relations into its body, typed over the call's own tables (RelationGenerics).
     */
    @Nested
    @DisplayName("Generic over relations: columns as parameters")
    class RelationGeneric {

        private static final String AGG = "AggColSpec<{Relation<T>[1],_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]";

        private static final String LIBRARY = """
                function test::rolled<T,K,V,R>(r: Relation<T>[1], win: _Window<T>[1], agg: %1$s): Relation<T+R>[1]
                {
                    $r->extend($win, $agg)
                }
                function test::running<T,X,Z,K,V,R>(r: Relation<T>[1], part: ColSpec<X⊆T>[1], order: SortInfo<Z⊆T>[*], agg: %1$s): Relation<T+R>[1]
                {
                    $r->extend(over($part, $order, rows(unbounded(), 0)), $agg)
                }
                function test::lastN<T,X,Z,K,V,R>(r: Relation<T>[1], part: ColSpec<X⊆T>[1], order: SortInfo<Z⊆T>[*], w: Integer[1], agg: %1$s): Relation<T+R>[1]
                {
                    $r->extend(over($part, $order, rows(1 - $w, 0)), $agg)
                }
                """.formatted(AGG);

        private static final String PEOPLE = "#>{store::PersonDatabase.T_PERSON}#";

        /** A query's rows as (first name, value) pairs, by first name. */
        private java.util.Map<String, Integer> byName(Lean.Result result, int name, int value) {
            java.util.Map<String, Integer> out = new java.util.TreeMap<>();
            for (var row : result.rows()) {
                out.put(String.valueOf(row.get(name)), ((Number) row.get(value)).intValue());
            }
            return out;
        }

        @Test
        @DisplayName("a window and an aggregate as parameters: the call's own, placed by the body")
        void windowAndAggregateAsParameters() throws SQLException {
            var result = exec(modelWith(LIBRARY), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::rolled(over(~LAST_NAME, [~AGE_VAL->ascending()], rows(unbounded(), 0)), ~total:{p,w,r|$r.AGE_VAL}:y|$y->sum())");
            // Smith: Jane 28, then John 28 + 30; Jones: Bob 45
            assertEquals(java.util.Map.of("Bob", 45, "Jane", 28, "John", 58), byName(result, 0, 3));
        }

        @Test
        @DisplayName("columns as parameters: the partition and the sort the caller names")
        void columnsAsParameters() throws SQLException {
            var result = exec(modelWith(LIBRARY), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::running(~LAST_NAME, [~AGE_VAL->ascending()], ~total:{p,w,r|$r.AGE_VAL}:y|$y->sum())");
            assertEquals(java.util.Map.of("Bob", 45, "Jane", 28, "John", 58), byName(result, 0, 3));
        }

        @Test
        @DisplayName("a window's length as a parameter: rows(1 - $w, 0) over the call's own w")
        void windowLengthAsParameter() throws SQLException {
            var result = exec(modelWith(LIBRARY), "|" + PEOPLE + "->select(~[FIRST_NAME, AGE_VAL])->extend(~all:x|'all')"
                    + "->test::lastN(~all, [~AGE_VAL->ascending()], 2, ~two:{p,w,r|$r.AGE_VAL}:y|$y->sum())");
            // by age 28, 30, 45: 28, 28 + 30, 30 + 45
            assertEquals(java.util.Map.of("Jane", 28, "John", 58, "Bob", 75), byName(result, 0, 3));
        }

        @Test
        @DisplayName("one function nested in its own argument: expanded within itself, never recursion")
        void nestedInItsOwnArgument() throws SQLException {
            var result = exec(modelWith(LIBRARY), "|" + PEOPLE + "->select(~[FIRST_NAME, AGE_VAL])->extend(~all:x|'all')"
                    + "->test::lastN(~all, [~AGE_VAL->ascending()], 2, ~two:{p,w,r|$r.AGE_VAL}:y|$y->sum())"
                    + "->test::lastN(~all, [~AGE_VAL->ascending()], 2, ~four:{p,w,r|$r.two}:y|$y->sum())");
            // two: 28, 58, 75; four: 28, 28 + 58, 58 + 75
            assertEquals(java.util.Map.of("Jane", 28, "John", 86, "Bob", 133), byName(result, 0, 4));
        }

        @Test
        @DisplayName("over any table: the same function over another table's columns")
        void overAnyTable() throws SQLException {
            // the database declares T_EMPLOYEE too, a table of other columns
            String model = modelWith(LIBRARY).replace("Table T_PERSON (", """
                    Table T_EMPLOYEE (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100), AGE_VAL INTEGER, DEPT VARCHAR(100))
                    Table T_PERSON (""");
            var result = exec(model, "|#>{store::PersonDatabase.T_EMPLOYEE}#->select(~[FIRST_NAME, DEPT, AGE_VAL])"
                    + "->test::running(~DEPT, [~AGE_VAL->ascending()], ~n:{p,w,r|$r.AGE_VAL}:y|$y->count())");
            assertEquals(java.util.Map.of("Alice", 1, "Charlie", 1), byName(result, 0, 3));
        }

        @Test
        @DisplayName("a column the table has not: refused at the call, by name")
        void columnTheTableHasNot() {
            var ex = assertThrows(Exception.class, () -> exec(modelWith(LIBRARY), "|" + PEOPLE + "->select(~[FIRST_NAME, AGE_VAL])"
                    + "->test::running(~NOPE, [~AGE_VAL->ascending()], ~total:{p,w,r|$r.AGE_VAL}:y|$y->sum())"));
            assertTrue(ex.getMessage().contains("NOPE"), ex.getMessage());
        }

        @Test
        @DisplayName("a body calling itself: still refused, loudly")
        void recursionStillLoud() {
            String model = modelWith("""
                    function test::loop<T>(r: Relation<T>[1]): Relation<T>[1]
                    {
                        $r->test::loop()
                    }
                    """);
            var ex = assertThrows(Exception.class, () -> exec(model, "|" + PEOPLE + "->select(~[FIRST_NAME])->test::loop()"));
            assertTrue(ex.getMessage().contains("recursive"), ex.getMessage());
        }

        @Test
        @DisplayName("the model compiles whole: a generic body is typed at its calls, so it reports no error of its own")
        void compilesWhole() {
            assertEquals(List.of(), com.legend.Compiler.compileErrors(modelWith(LIBRARY)));
        }

        /** A function of a COLUMN by its name (the DataCube library's form, datacube::ts::*): the column read and the
         *  column written are the call's, renamed to the body's own names and back. */
        private static final String NAMED = """
                function test::mean<T,X,Z,V,O>(rel: Relation<T>[1], part: ColSpec<X⊆T>[1], order: SortInfo<Z⊆T>[*], value: ColSpec<V⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<Any>[1]
                {
                    $rel->rename($value, ~ts_v)
                        ->extend(over($part, $order, rows(1 - $w, 0)), ~ts_o:{p,f,r|$r.ts_v}:y|if($y->count() < $w, |[], |$y->average()))
                        ->rename(~ts_v, $value)
                        ->rename(~ts_o, $out)
                }
                """;

        @Test
        @DisplayName("a column read and a column written by name: the call's own, through the body's")
        void columnsByName() throws SQLException {
            var result = exec(modelWith(NAMED), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::mean(~LAST_NAME, [~FIRST_NAME->ascending()], ~AGE_VAL, 2, ~avg)->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "avg"), result.columns().stream().map(c -> c.name()).toList());
            // Smith: Jane alone in her window (none), John with Jane (29); Jones: Bob alone
            assertEquals(java.util.Arrays.asList(null, null, 29.0), result.rows().stream()
                    .map(r -> r.get(3) == null ? null : ((Number) r.get(3)).doubleValue()).toList());
        }

        @Test
        @DisplayName("a call by name nested in its own: the mean of a mean, each its own columns")
        void columnsByNameNested() throws SQLException {
            var result = exec(modelWith(NAMED), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::mean(~LAST_NAME, [~FIRST_NAME->ascending()], ~AGE_VAL, 1, ~a)"
                    + "->test::mean(~LAST_NAME, [~FIRST_NAME->ascending()], ~a, 2, ~b)->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "a", "b"), result.columns().stream().map(c -> c.name()).toList());
            assertEquals(java.util.Arrays.asList(null, null, 29.0), result.rows().stream()
                    .map(r -> r.get(4) == null ? null : ((Number) r.get(4)).doubleValue()).toList());
        }

        /** A function generic over any table that drops the working column it made ({@code without}). */
        private static final String FROM_MEAN = """
                function test::fromMean<T,X,V,O>(r: Relation<T>[1], part: ColSpec<X⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
                {
                    $r->rename($value, ~t_v)
                      ->extend(over($part), ~t_m:{p,w,r|$r.t_v}:y|$y->average())
                      ->extend(~t_o:x|$x.t_v->toOne() - $x.t_m->toOne())
                      ->without(~t_m)
                      ->rename(~t_v, $value)
                      ->rename(~t_o, $out)
                }
                """;

        @Test
        @DisplayName("a body drops the columns it made (without): the answer is the call's table and its own column")
        void dropsItsWorkingColumns() throws SQLException {
            var result = exec(modelWith(FROM_MEAN), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::fromMean(~LAST_NAME, ~AGE_VAL, ~gap)->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "gap"), result.columns().stream().map(c -> c.name()).toList());
            // Smith: Jane 28 and John 30 about 29; Jones: Bob 45 alone
            assertEquals(List.of(0.0, -1.0, 1.0), result.rows().stream().map(r -> ((Number) r.get(3)).doubleValue()).toList());
        }

        @Test
        @DisplayName("hygiene: a caller's columns named as the body's working ones are the caller's, untouched")
        void callerColumnsOfWorkingNames() throws SQLException {
            var result = exec(modelWith(FROM_MEAN), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->extend(~[t_m:x|7, t_v:x|'mine'])->test::fromMean(~LAST_NAME, ~AGE_VAL, ~gap)"
                    + "->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "t_m", "t_v", "gap"),
                    result.columns().stream().map(c -> c.name()).toList());
            assertEquals(List.of(7, 7, 7), result.rows().stream().map(r -> ((Number) r.get(3)).intValue()).toList());
            assertEquals(List.of("mine", "mine", "mine"), result.rows().stream().map(r -> String.valueOf(r.get(4))).toList());
            assertEquals(List.of(0.0, -1.0, 1.0), result.rows().stream().map(r -> ((Number) r.get(5)).doubleValue()).toList());
        }

        @Test
        @DisplayName("hygiene: a function over its own answer, each expansion's working columns its own")
        void overItsOwnAnswer() throws SQLException {
            var result = exec(modelWith(FROM_MEAN), "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::fromMean(~LAST_NAME, ~AGE_VAL, ~gap)->test::fromMean(~LAST_NAME, ~gap, ~gap2)"
                    + "->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "gap", "gap2"),
                    result.columns().stream().map(c -> c.name()).toList());
            // each line's gaps average 0, so the second gap is the first
            assertEquals(List.of(0.0, -1.0, 1.0), result.rows().stream().map(r -> ((Number) r.get(4)).doubleValue()).toList());
        }

        @Test
        @DisplayName("hygiene: a column the declared return names leaves under its own name")
        void aDeclaredColumnKeepsItsName() throws SQLException {
            String model = modelWith("""
                    function test::counted<T>(r: Relation<T>[1]): Relation<T+(n:Integer)>[1]
                    {
                        $r->extend(~[n:x|1, t_w:x|2])->without(~t_w)
                    }
                    """);
            var result = exec(model, "|" + PEOPLE + "->select(~[FIRST_NAME])->extend(~t_w:x|3)->test::counted()");
            assertEquals(List.of("FIRST_NAME", "t_w", "n"), result.columns().stream().map(c -> c.name()).toList());
            assertEquals(List.of(3, 3, 3), result.rows().stream().map(r -> ((Number) r.get(1)).intValue()).toList());
        }

        @Test
        @DisplayName("read: a function takes its window whole and reads the column its caller names, nothing renamed")
        void aWholeWindowAndARead() throws SQLException {
            String model = modelWith("""
                    function test::meanOf<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
                    {
                        $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->average()))->rename(~m, $out)
                    }
                    """);
            var result = exec(model, "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])"
                    + "->test::meanOf(over(~LAST_NAME, [~FIRST_NAME->ascending()], rows(-1, 0)), ~AGE_VAL, 2, ~m2)"
                    + "->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "m2"), result.columns().stream().map(c -> c.name()).toList());
            // Jones: Bob alone; Smith: Jane first (one value), then John with Jane: 29
            assertEquals(java.util.Arrays.asList(null, null, 29.0), result.rows().stream()
                    .map(r -> r.get(3) == null ? null : ((Number) r.get(3)).doubleValue()).toList());
        }

        @Test
        @DisplayName("an option an enumeration: the type checker refuses a value it has not, and the branch it names alone reaches the SQL")
        void anOptionAnEnumeration() throws SQLException {
            // the user, 2026-10-28 ("I hope it is the second": checked when the query compiles, never in its SQL): a
            // function's option typed by an enumeration -- a value it has not refused by the type checker, and an if on
            // it folded to the branch the call's value names, as a spelled string's is (StaticFold.EnumToken)
            String model = modelWith("""
                    Enum test::Stat
                    {
                      mean, total, most
                    }
                    function test::statOf<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], stat: test::Stat[1], out: ColSpec<O>[1]): Relation<T+O>[1]
                    {
                        $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($stat == test::Stat.mean, |$y->average(), |if($stat == test::Stat.total, |$y->sum(), |$y->max())))->rename(~m, $out)
                    }
                    """);
            String over = "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])->test::statOf(over(~LAST_NAME), ~AGE_VAL, test::Stat.";
            var total = exec(model, over + "total, ~s)->sort(~FIRST_NAME->ascending())");
            // Jones: Bob 45; Smith: Jane and John 28 + 30
            assertEquals(List.of(45, 58, 58), total.rows().stream().map(r -> ((Number) r.get(3)).intValue()).toList());
            String sql = plan(model, over + "total, ~s)").sql().toUpperCase(java.util.Locale.ROOT);
            assertTrue(sql.contains("SUM("), sql);
            assertFalse(sql.contains("AVG(") || sql.contains("MAX(") || sql.contains("CASE"), "the branch it names alone: " + sql);
            var refused = assertThrows(Exception.class, () -> plan(model, over + "Total, ~s)"));
            assertTrue(refused.getMessage().contains("enumeration test::Stat has no value 'Total'"), refused.getMessage());
        }

        @Test
        @DisplayName("an enumeration's value a body passes on whole is written back as the call spelled it: a helper's parameter, a map's element")
        void anEnumerationPassedOnWhole() throws SQLException {
            // the independent review, 2026-10-28, and the corpus's testExtendDigest_Relational (lost the day StaticFold
            // first folded an enumeration's value): a value bound to a helper's parameter, or to the element of a map it
            // unrolls, and used whole (hash($h)) was left a variable no binder held
            String model = modelWith("""
                    function test::digest(s: String[1], h: meta::pure::functions::hash::HashType[1]): String[1]
                    {
                        $s->hash($h)
                    }
                    function test::digestsOf<T,O,P>(t: Relation<T>[1], one: ColSpec<O>[1], both: ColSpec<P>[1]): Relation<T+O+P>[1]
                    {
                        $t->extend(~d1: r|test::digest('x', meta::pure::functions::hash::HashType.MD5))
                          ->extend(~d2: r|[meta::pure::functions::hash::HashType.MD5, meta::pure::functions::hash::HashType.SHA1]->map(h|'x'->hash($h))->joinStrings('|'))
                          ->rename(~d1, $one)->rename(~d2, $both)
                    }
                    """);
            var result = exec(model, "|" + PEOPLE + "->select(~[FIRST_NAME])->test::digestsOf(~one, ~both)->sort(~FIRST_NAME->ascending())->limit(1)");
            // md5('x') and sha1('x')
            assertEquals("9dd4e461268c8034f5c8564e155c67a6", result.rows().get(0).get(1));
            assertEquals("9dd4e461268c8034f5c8564e155c67a6|11f6ad8ec52a2984abaafd7c3b516503785c2072", result.rows().get(0).get(2));
        }

        @Test
        @DisplayName("an enumeration's value a generic body compares a mapped column to reads as the database's code")
        void anEnumerationMappedToTheDatabasesCodes() throws SQLException {
            // the user, 2026-10-28 ("enums that have mappings like in the database its 'A' but the Enum is ACTIVE"): the
            // static fold writes the enumeration's value back as the call spelled it, and the mapping, not the fold,
            // reads it as the database's code -- here a family's name, 'Smith', for model::Family.SMITH
            String mapping = """

                    ###Mapping
                    import store::*;
                    Mapping model::FamilyMapping
                    (
                        model::Member: Relational
                        {
                            ~mainTable [PersonDatabase] T_PERSON
                            firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                            family: EnumerationMapping FamilyNames: [PersonDatabase] T_PERSON.LAST_NAME
                        }
                        model::Family: EnumerationMapping FamilyNames
                        {
                            SMITH: 'Smith',
                            JONES: 'Jones'
                        }
                    )
                    """;
            String functions = """
                    Enum model::Family
                    {
                      SMITH, JONES
                    }
                    Class model::Member
                    {
                        firstName: String[1];
                        family: model::Family[1];
                    }
                    function test::sameAs(f: model::Family[1], wanted: model::Family[1]): Boolean[1]
                    {
                        $f == $wanted
                    }
                    function test::ofFamily<T,F>(t: Relation<T>[1], col: ColSpec<F⊆T>[1], wanted: model::Family[1]): Relation<T>[1]
                    {
                        $t->filter(r|test::sameAs($r->read($col), $wanted))
                    }
                    """;
            String model = BASE_MODEL.replace("mappings: [ model::PersonMapping ];", "mappings: [ model::FamilyMapping ];")
                    + mapping + "\n###Pure\n" + functions;
            String query = "|model::Member.all()->project(~[firstName: m|$m.firstName, family: m|$m.family])"
                    + "->test::ofFamily(~family, model::Family.SMITH)->sort(~firstName->ascending())";
            var result = exec(model, query);
            assertEquals(List.of("Jane", "John"), result.rows().stream().map(r -> (String) r.get(0)).toList());
            assertEquals(List.of("SMITH", "SMITH"), result.rows().stream().map(r -> String.valueOf(r.get(1))).toList());
            // the filter on the column as the database holds it, the column read back as the enumeration's value
            String sql = plan(model, query).sql();
            assertTrue(java.util.regex.Pattern.compile("WHERE\\s+\\w+\\.LAST_NAME = 'Smith'").matcher(sql).find(), sql);
            assertTrue(sql.contains("THEN 'SMITH'"), sql);
        }

        @Test
        @DisplayName("a chain of calls of a generic function plans once each: twenty calls, each over the answer of the last")
        void aChainOfCallsIsTypedOnceEach() throws SQLException {
            // the focused Pure audit, 2026-10-28: each call typed the chain under it twice -- its check and its inlined
            // body -- so a chain of k calls planned in 2^k (sixteen in 13 s), and past sixteen was refused as an
            // expansion of the function within itself; each table a call typed is typed once
            String model = modelWith("""
                    function test::meanOf<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
                    {
                        $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->average()))->rename(~m, $out)
                    }
                    """);
            StringBuilder q = new StringBuilder("|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])");
            String prev = "AGE_VAL";
            for (int i = 0; i < 20; i++) {
                q.append("->test::meanOf(over(~LAST_NAME, [~FIRST_NAME->ascending()], rows(-1, 0)), ~").append(prev).append(", 1, ~m").append(i).append(")");
                prev = "m" + i;
            }
            long t0 = System.nanoTime();
            plan(model, q + "->sort(~FIRST_NAME->ascending())");
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 5_000, "twenty calls planned in " + ms + " ms");
            // and its answer each call's: Bob alone (45 throughout); Jane first (28 throughout); John the mean of his and
            // Jane's before, each call halving his distance from her (30, then 29, 28.5, ...: 28 + 2^-19 at the twentieth)
            var result = exec(model, q + "->sort(~FIRST_NAME->ascending())");
            List<Double> last = result.rows().stream().map(r -> ((Number) r.get(22)).doubleValue()).toList();
            assertEquals(45.0, last.get(0), 1e-12);
            assertEquals(28.0, last.get(1), 1e-12);
            assertEquals(28.0 + Math.pow(2, -19), last.get(2), 1e-12);
        }

        @Test
        @DisplayName("hygiene: the column a body has another generic function write for its own use is the body's own")
        void aNestedCallsColumnIsTheBodys() throws SQLException {
            String model = modelWith("""
                    function test::meanOf<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
                    {
                        $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->average()))->rename(~m, $out)
                    }
                    function test::offItsMean<T,L,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
                    {
                        $t->test::meanOf(over($line), $value, 1, ~m)->extend(~g:x|$x->read($value)->toOne() - $x.m->toOne())->without(~m)->rename(~g, $out)
                    }
                    """);
            // the caller's table has an m of its own, which the body's m (meanOf's out) must not meet
            var result = exec(model, "|" + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])->extend(~m:x|'mine')"
                    + "->test::offItsMean(~LAST_NAME, ~AGE_VAL, ~gap)->sort(~FIRST_NAME->ascending())");
            assertEquals(List.of("FIRST_NAME", "LAST_NAME", "AGE_VAL", "m", "gap"), result.columns().stream().map(c -> c.name()).toList());
            assertEquals(List.of("mine", "mine", "mine"), result.rows().stream().map(r -> String.valueOf(r.get(3))).toList());
            // Jones: Bob alone; Smith: Jane 28 and John 30 about their 29
            assertEquals(List.of(0.0, -1.0, 1.0), result.rows().stream().map(r -> ((Number) r.get(4)).doubleValue()).toList());
        }

        @Test
        @DisplayName("a declared return is held to each call: a body that answers no table is refused by name")
        void aBodyOfNoTableIsRefused() {
            String model = modelWith("""
                    function test::notATable<T>(t: Relation<T>[1]): Relation<T>[1]
                    {
                        1
                    }
                    """);
            var ex = assertThrows(Exception.class, () -> exec(model, "|" + PEOPLE + "->select(~[FIRST_NAME])->test::notATable()"));
            assertTrue(ex.getMessage().contains("test::notATable") && ex.getMessage().contains("is a table"), ex.getMessage());
        }

        /** fromMean with its drop replaced: the same body, its answer {@code drop}'s. */
        private String fromMeanThat(String drop) {
            return modelWith(FROM_MEAN.replace("->without(~t_m)", drop));
        }

        @Test
        @DisplayName("a declared return is held to each call: a working column left in the answer is refused by name")
        void aLeakedColumnIsRefused() {
            var ex = assertThrows(Exception.class, () -> exec(fromMeanThat(""), "|" + PEOPLE
                    + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])->test::fromMean(~LAST_NAME, ~AGE_VAL, ~gap)"));
            assertTrue(ex.getMessage().contains("test::fromMean") && ex.getMessage().contains("[t_m] not declared"),
                    ex.getMessage());
        }

        @Test
        @DisplayName("a declared return is held to each call: a column of the caller's the body loses is refused by name")
        void aLostColumnIsRefused() {
            var ex = assertThrows(Exception.class, () -> exec(fromMeanThat("->without(~[t_m, FIRST_NAME])"), "|"
                    + PEOPLE + "->select(~[FIRST_NAME, LAST_NAME, AGE_VAL])->test::fromMean(~LAST_NAME, ~AGE_VAL, ~gap)"));
            assertTrue(ex.getMessage().contains("test::fromMean") && ex.getMessage().contains("FIRST_NAME"),
                    ex.getMessage());
        }
    }
}
