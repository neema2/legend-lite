package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for ExtendChecker — all overloads.
 * <ul>
 *   <li>Scalar extend: column access, arithmetic, string functions</li>
 *   <li>Window ranking: rowNumber, rank, denseRank, ntile, percentRank, cumeDist</li>
 *   <li>Window offset: lag, lead, first, last</li>
 *   <li>Window aggregate: sum, count, avg, min, max, stdDev, variance with over()</li>
 *   <li>Frame permutations: rows/range × unbounded/offset/currentRow</li>
 *   <li>Over clause permutations: partition/order/frame combos</li>
 *   <li>Chaining: extend→filter, extend→sort, extend→extend, etc.</li>
 *   <li>Edge cases and error handling</li>
 * </ul>
 */
public class ExtendCheckerTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() { return "DuckDB"; }

    @Override
    protected SQLDialect getDialect() { return DuckDBDialect.INSTANCE; }

    @Override
    protected String getJdbcUrl() { return "jdbc:duckdb:"; }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ========================================================================
    // 1. Scalar extend — single column expressions
    // ========================================================================

    @Nested
    @DisplayName("extend(~col: x | expr)")
    class ScalarExtend {

        @Test
        @DisplayName("extend single column access")
        void testSingleColumnAccess() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->extend(~copy: x | $x.name)""");
            assertEquals(2, result.rowCount());
            assertEquals(3, result.columns().size());
            assertEquals("copy", result.columns().get(2).name());
        }

        @Test
        @DisplayName("extend multiple columns via array syntax")
        void testMultipleColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, val
                    1, 10
                    2, 20
                    #->extend(~[a: x | $x.val, b: x | $x.id])""");
            assertEquals(2, result.rowCount());
            assertEquals(4, result.columns().size());
            assertEquals("a", result.columns().get(2).name());
            assertEquals("b", result.columns().get(3).name());
        }

        @Test
        @DisplayName("extend with integer literal")
        void testIntegerLiteral() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id
                    1
                    2
                    3
                    #->extend(~constant: x | 42)""");
            assertEquals(3, result.rowCount());
            for (var row : result.rows()) {
                assertEquals(42L, ((Number) row.get(colIdx(result, "constant"))).longValue());
            }
        }

        @Test
        @DisplayName("extend with string literal")
        void testStringLiteral() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id
                    1
                    2
                    #->extend(~label: x | 'hello')""");
            assertEquals(2, result.rowCount());
            for (var row : result.rows()) {
                assertEquals("hello", row.get(colIdx(result, "label")));
            }
        }

        @Test
        @DisplayName("extend with float literal")
        void testFloatLiteral() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id
                    1
                    #->extend(~pi: x | 3.14)""");
            assertEquals(1, result.rowCount());
            assertEquals(3.14, ((Number) result.rows().get(0).get(colIdx(result, "pi"))).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("extend rejects duplicate column name")
        void testOverrideColumn() {
            assertThrows(com.gs.legend.compiler.PureCompileException.class, () ->
                executeRelation("""
                    |#TDS
                    id, val
                    1, 10
                    2, 20
                    #->extend(~val: x | $x.val + 100)"""));
        }

        @Test
        @DisplayName("extend single column, no array syntax")
        void testSingleColumnNoArray() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b
                    1, 10
                    2, 20
                    #->extend(~total: x | $x.a + $x.b)""");
            assertEquals(2, result.rowCount());
            int idx = colIdx(result, "total");
            assertEquals(11L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(22L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("extend three columns via array")
        void testThreeColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    x
                    5
                    #->extend(~[a: r | $r.x + 1, b: r | $r.x + 2, c: r | $r.x + 3])""");
            assertEquals(1, result.rowCount());
            assertEquals(4, result.columns().size());
            assertEquals(6L, ((Number) result.rows().get(0).get(colIdx(result, "a"))).longValue());
            assertEquals(7L, ((Number) result.rows().get(0).get(colIdx(result, "b"))).longValue());
            assertEquals(8L, ((Number) result.rows().get(0).get(colIdx(result, "c"))).longValue());
        }

        @Test
        @DisplayName("extend preserves original columns")
        void testPreservesOriginalColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, val
                    1, Alice, 100
                    #->extend(~doubled: x | $x.val * 2)""");
            assertEquals(4, result.columns().size());
            assertEquals("id", result.columns().get(0).name());
            assertEquals("name", result.columns().get(1).name());
            assertEquals("val", result.columns().get(2).name());
            assertEquals("doubled", result.columns().get(3).name());
        }

        @Test
        @DisplayName("extend on single row")
        void testSingleRow() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    42
                    #->extend(~sq: x | $x.val * $x.val)""");
            assertEquals(1, result.rowCount());
            assertEquals(1764L, ((Number) result.rows().get(0).get(colIdx(result, "sq"))).longValue());
        }
    }

    // ========================================================================
    // 2. Arithmetic extend — math expressions
    // ========================================================================

    @Nested
    @DisplayName("extend(~col: x | arithmetic)")
    class ArithmeticExtend {

        @Test
        @DisplayName("addition")
        void testAddition() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b
                    10, 3
                    20, 7
                    #->extend(~sum: x | $x.a + $x.b)""");
            int idx = colIdx(result, "sum");
            assertEquals(13L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(27L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("subtraction")
        void testSubtraction() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b
                    10, 3
                    20, 7
                    #->extend(~diff: x | $x.a - $x.b)""");
            int idx = colIdx(result, "diff");
            assertEquals(7L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(13L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("multiplication")
        void testMultiplication() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    price, qty
                    10, 5
                    20, 3
                    #->extend(~revenue: x | $x.price * $x.qty)""");
            int idx = colIdx(result, "revenue");
            assertEquals(50L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(60L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("division")
        void testDivision() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    total, count
                    100, 4
                    200, 5
                    #->extend(~avg: x | $x.total / $x.count)""");
            int idx = colIdx(result, "avg");
            assertEquals(25L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(40L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("nested arithmetic: (a + b) * c")
        void testNestedArithmetic() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b, c
                    2, 3, 4
                    #->extend(~val: x | ($x.a + $x.b) * $x.c)""");
            assertEquals(20L, ((Number) result.rows().get(0).get(colIdx(result, "val"))).longValue());
        }

        @Test
        @DisplayName("abs()")
        void testAbs() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    -5
                    3
                    0
                    #->extend(~absVal: x | $x.val->abs())""");
            int idx = colIdx(result, "absVal");
            assertEquals(5L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(3L, ((Number) result.rows().get(1).get(idx)).longValue());
            assertEquals(0L, ((Number) result.rows().get(2).get(idx)).longValue());
        }

        @Test
        @DisplayName("round()")
        void testRound() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    3.456
                    7.891
                    #->extend(~rounded: x | $x.val->round(1))""");
            int idx = colIdx(result, "rounded");
            assertEquals(3.5, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(7.9, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("ceiling()")
        void testCeiling() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    3.1
                    7.9
                    #->extend(~ceil: x | $x.val->ceiling())""");
            int idx = colIdx(result, "ceil");
            assertEquals(4L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(8L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("floor()")
        void testFloor() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    3.9
                    7.1
                    #->extend(~flr: x | $x.val->floor())""");
            int idx = colIdx(result, "flr");
            assertEquals(3L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(7L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("pow()")
        void testPow() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    base
                    2
                    3
                    #->extend(~squared: x | $x.base->pow(2))""");
            int idx = colIdx(result, "squared");
            assertEquals(4.0, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(9.0, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("sqrt()")
        void testSqrt() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    16
                    25
                    #->extend(~root: x | $x.val->sqrt())""");
            int idx = colIdx(result, "root");
            assertEquals(4.0, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(5.0, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("multiple arithmetic extends")
        void testMultipleExtends() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    #->extend(~[doubled: x | $x.val * 2, halved: x | $x.val / 2, negated: x | 0 - $x.val])""");
            assertEquals(20L, ((Number) result.rows().get(0).get(colIdx(result, "doubled"))).longValue());
            assertEquals(5L, ((Number) result.rows().get(0).get(colIdx(result, "halved"))).longValue());
            assertEquals(-10L, ((Number) result.rows().get(0).get(colIdx(result, "negated"))).longValue());
        }

        @Test
        @DisplayName("arithmetic with literal operands")
        void testLiteralOperands() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    #->extend(~plusTen: x | $x.val + 10)""");
            assertEquals(20L, ((Number) result.rows().get(0).get(colIdx(result, "plusTen"))).longValue());
        }

        @Test
        @DisplayName("chained math: abs of subtraction")
        void testChainedMath() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b
                    3, 10
                    10, 3
                    #->extend(~diff: x | ($x.a - $x.b)->abs())""");
            int idx = colIdx(result, "diff");
            assertEquals(7L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(7L, ((Number) result.rows().get(1).get(idx)).longValue());
        }
    }

    // ========================================================================
    // 3. String extend — string functions in fn1
    // ========================================================================

    @Nested
    @DisplayName("extend(~col: x | string-func)")
    class StringExtend {

        @Test
        @DisplayName("string concatenation via plus")
        void testConcat() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    first, last
                    Alice, Smith
                    Bob, Jones
                    #->extend(~full: x | $x.first + ' ' + $x.last)""");
            int idx = colIdx(result, "full");
            assertEquals("Alice Smith", result.rows().get(0).get(idx));
            assertEquals("Bob Jones", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("toUpper()")
        void testToUpper() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    name
                    alice
                    Bob
                    #->extend(~upper: x | $x.name->toUpper())""");
            int idx = colIdx(result, "upper");
            assertEquals("ALICE", result.rows().get(0).get(idx));
            assertEquals("BOB", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("toLower()")
        void testToLower() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    name
                    ALICE
                    Bob
                    #->extend(~lower: x | $x.name->toLower())""");
            int idx = colIdx(result, "lower");
            assertEquals("alice", result.rows().get(0).get(idx));
            assertEquals("bob", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("trim()")
        void testTrim() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    name
                     hello\s
                    world
                    #->extend(~trimmed: x | $x.name->trim())""");
            int idx = colIdx(result, "trimmed");
            assertEquals("hello", result.rows().get(0).get(idx));
            assertEquals("world", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("contains()")
        void testContains() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello world
                    goodbye
                    #->extend(~hasHello: x | $x.str->contains('hello'))""");
            int idx = colIdx(result, "hasHello");
            assertEquals(true, result.rows().get(0).get(idx));
            assertEquals(false, result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("startsWith()")
        void testStartsWith() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello world
                    goodbye
                    #->extend(~startsH: x | $x.str->startsWith('hello'))""");
            int idx = colIdx(result, "startsH");
            assertEquals(true, result.rows().get(0).get(idx));
            assertEquals(false, result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("endsWith()")
        void testEndsWith() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello world
                    goodbye
                    #->extend(~endsWorld: x | $x.str->endsWith('world'))""");
            int idx = colIdx(result, "endsWorld");
            assertEquals(true, result.rows().get(0).get(idx));
            assertEquals(false, result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("length()")
        void testLength() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello
                    hi
                    #->extend(~len: x | $x.str->length())""");
            int idx = colIdx(result, "len");
            assertEquals(5L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(2L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("toString()")
        void testToString() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    42
                    7
                    #->extend(~str: x | $x.val->toString())""");
            int idx = colIdx(result, "str");
            assertEquals("42", result.rows().get(0).get(idx));
            assertEquals("7", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("parseInteger()")
        void testParseInteger() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str:String
                    42
                    99
                    #->extend(~num: x | $x.str->parseInteger())""");
            int idx = colIdx(result, "num");
            assertEquals(42L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(99L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("parseFloat()")
        void testParseFloat() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str:String
                    3.14
                    2.71
                    #->extend(~num: x | $x.str->parseFloat())""");
            int idx = colIdx(result, "num");
            assertEquals(3.14, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(2.71, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("replace()")
        void testReplace() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello world
                    foo bar
                    #->extend(~replaced: x | $x.str->replace('o', '0'))""");
            int idx = colIdx(result, "replaced");
            assertEquals("hell0 w0rld", result.rows().get(0).get(idx));
            assertEquals("f00 bar", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("string concat with toString")
        void testConcatWithToString() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    name, val
                    Alice, 1
                    Bob, 2
                    #->extend(~label: x | $x.name + $x.val->toString())""");
            int idx = colIdx(result, "label");
            assertEquals("Alice1", result.rows().get(0).get(idx));
            assertEquals("Bob2", result.rows().get(1).get(idx));
        }

        @Test
        @DisplayName("indexOf()")
        void testIndexOf() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello world
                    abc
                    #->extend(~pos: x | $x.str->indexOf('o'))""");
            int idx = colIdx(result, "pos");
            assertEquals(4L, ((Number) result.rows().get(0).get(idx)).longValue());
        }

        @Test
        @DisplayName("substring()")
        void testSubstring() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str
                    hello world
                    #->extend(~sub: x | $x.str->substring(0, 5))""");
            int idx = colIdx(result, "sub");
            assertEquals("hello", result.rows().get(0).get(idx));
        }
    }

    // ========================================================================
    // 4. Type coercion and cast
    // ========================================================================

    @Nested
    @DisplayName("extend with cast/type coercion")
    class TypeCoercionExtend {

        @Test
        @DisplayName("integer arithmetic preserves integer type")
        void testIntegerPreservation() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    #->extend(~doubled: x | $x.val * 2)""");
            int idx = colIdx(result, "doubled");
            assertEquals(20L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(40L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test
        @DisplayName("integer to string conversion")
        void testIntToString() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    42
                    #->extend(~str: x | $x.val->toString())""");
            assertEquals("42", result.rows().get(0).get(colIdx(result, "str")));
        }

        @Test
        @DisplayName("float arithmetic preserves float type")
        void testFloatPreservation() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    1.5
                    2.5
                    #->extend(~doubled: x | $x.val * 2)""");
            int idx = colIdx(result, "doubled");
            assertEquals(3.0, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(5.0, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("mixed int+float yields float")
        void testMixedTypes() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    intVal, floatVal
                    10, 3.5
                    #->extend(~sum: x | $x.intVal + $x.floatVal)""");
            int idx = colIdx(result, "sum");
            assertEquals(13.5, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("string to integer conversion")
        void testStringToInt() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    str:String
                    42
                    #->extend(~num: x | $x.str->parseInteger())""");
            assertEquals(42L, ((Number) result.rows().get(0).get(colIdx(result, "num"))).longValue());
        }

        @Test
        @DisplayName("boolean from comparison in extend")
        void testBooleanFromComparison() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    5
                    #->extend(~big: x | $x.val > 15)""");
            int idx = colIdx(result, "big");
            assertEquals(false, result.rows().get(0).get(idx));
            assertEquals(true, result.rows().get(1).get(idx));
            assertEquals(false, result.rows().get(2).get(idx));
        }

        @Test
        @DisplayName("integer division preserves integer")
        void testIntegerDivision() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    100
                    #->extend(~half: x | $x.val / 2)""");
            assertEquals(50L, ((Number) result.rows().get(0).get(colIdx(result, "half"))).longValue());
        }
    }

    // ========================================================================
    // 5. Math extended — cbrt, exp, log, log10, sign, mod, rem, pi
    // ========================================================================

    @Nested
    @DisplayName("extend — math extended")
    class MathExtend {

        @Test @DisplayName("cbrt()")
        void testCbrt() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    27
                    8
                    #->extend(~cube: x | $x.val->cbrt())""");
            int idx = colIdx(result, "cube");
            assertEquals(3.0, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(2.0, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test @DisplayName("exp()")
        void testExp() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    0
                    1
                    #->extend(~e: x | $x.val->exp())""");
            int idx = colIdx(result, "e");
            assertEquals(1.0, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(Math.E, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test @DisplayName("log() — natural log")
        void testLog() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    1
                    #->extend(~ln: x | $x.val->log())""");
            assertEquals(0.0, ((Number) result.rows().get(0).get(colIdx(result, "ln"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("log10()")
        void testLog10() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    100
                    1000
                    #->extend(~l: x | $x.val->log10())""");
            int idx = colIdx(result, "l");
            assertEquals(2.0, ((Number) result.rows().get(0).get(idx)).doubleValue(), 0.001);
            assertEquals(3.0, ((Number) result.rows().get(1).get(idx)).doubleValue(), 0.001);
        }

        @Test @DisplayName("sign()")
        void testSign() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    -5
                    0
                    7
                    #->extend(~s: x | $x.val->sign())""");
            int idx = colIdx(result, "s");
            assertEquals(-1L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(0L, ((Number) result.rows().get(1).get(idx)).longValue());
            assertEquals(1L, ((Number) result.rows().get(2).get(idx)).longValue());
        }

        @Test @DisplayName("mod()")
        void testMod() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    7
                    #->extend(~m: x | $x.val->mod(3))""");
            int idx = colIdx(result, "m");
            assertEquals(1L, ((Number) result.rows().get(0).get(idx)).longValue());
            assertEquals(1L, ((Number) result.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("rem()")
        void testRem() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    #->extend(~r: x | $x.val->rem(3))""");
            assertEquals(1L, ((Number) result.rows().get(0).get(colIdx(result, "r"))).longValue());
        }
    }

    // ========================================================================
    // 6. Trigonometry
    // ========================================================================

    @Nested
    @DisplayName("extend — trigonometry")
    class TrigExtend {

        @Test @DisplayName("sin(0) = 0")
        void testSin() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~s: x | $x.val->sin())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "s"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("cos(0) = 1")
        void testCos() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~c: x | $x.val->cos())");
            assertEquals(1.0, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("tan(0) = 0")
        void testTan() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~t: x | $x.val->tan())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "t"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("asin(0) = 0")
        void testAsin() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~a: x | $x.val->asin())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "a"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("acos(1) = 0")
        void testAcos() throws SQLException {
            var r = executeRelation("|#TDS\nval\n1\n#->extend(~a: x | $x.val->acos())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "a"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("atan(0) = 0")
        void testAtan() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~a: x | $x.val->atan())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "a"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("atan2(0, 1) = 0")
        void testAtan2() throws SQLException {
            var r = executeRelation("|#TDS\ny, x\n0, 1\n#->extend(~a: r | $r.y->atan2($r.x))");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "a"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("sinh(0) = 0")
        void testSinh() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~s: x | $x.val->sinh())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "s"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("cosh(0) = 1")
        void testCosh() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~c: x | $x.val->cosh())");
            assertEquals(1.0, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("tanh(0) = 0")
        void testTanh() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0\n#->extend(~t: x | $x.val->tanh())");
            assertEquals(0.0, ((Number) r.rows().get(0).get(colIdx(r, "t"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("cot — cot(pi/4) ≈ 1")
        void testCot() throws SQLException {
            var r = executeRelation("|#TDS\nval\n0.785398\n#->extend(~c: x | $x.val->cot())");
            assertEquals(1.0, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.01);
        }

        @Test @DisplayName("toDegrees(pi) ≈ 180")
        void testToDegrees() throws SQLException {
            var r = executeRelation("|#TDS\nval\n3.14159265\n#->extend(~d: x | $x.val->toDegrees())");
            assertEquals(180.0, ((Number) r.rows().get(0).get(colIdx(r, "d"))).doubleValue(), 0.01);
        }

        @Test @DisplayName("toRadians(180) ≈ pi")
        void testToRadians() throws SQLException {
            var r = executeRelation("|#TDS\nval\n180\n#->extend(~rad: x | $x.val->toRadians())");
            assertEquals(Math.PI, ((Number) r.rows().get(0).get(colIdx(r, "rad"))).doubleValue(), 0.001);
        }
    }

    // ========================================================================
    // 7. Arithmetic type overloads
    // ========================================================================

    @Nested
    @DisplayName("extend — arithmetic type overloads")
    class ArithmeticOverloads {

        // === plus ===
        @Test @DisplayName("plus(Integer, Integer)")
        void testPlusIntInt() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 20\n#->extend(~c: x | $x.a + $x.b)");
            assertEquals(30L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("plus(Float, Float)")
        void testPlusFloatFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n1.5, 2.5\n#->extend(~c: x | $x.a + $x.b)");
            assertEquals(4.0, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("plus(Integer, Float) — mixed")
        void testPlusIntFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 2.5\n#->extend(~c: x | $x.a + $x.b)");
            assertEquals(12.5, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("plus(String, String) — concat")
        void testPlusStringString() throws SQLException {
            var r = executeRelation("|#TDS\na, b\nhello, world\n#->extend(~c: x | $x.a + $x.b)");
            assertEquals("helloworld", r.rows().get(0).get(colIdx(r, "c")));
        }

        @Test @DisplayName("plus(Integer, literal)")
        void testPlusIntLiteral() throws SQLException {
            var r = executeRelation("|#TDS\na\n5\n#->extend(~c: x | $x.a + 100)");
            assertEquals(105L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        // === minus ===
        @Test @DisplayName("minus(Integer, Integer)")
        void testMinusIntInt() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n20, 7\n#->extend(~c: x | $x.a - $x.b)");
            assertEquals(13L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("minus(Float, Float)")
        void testMinusFloatFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n5.5, 2.0\n#->extend(~c: x | $x.a - $x.b)");
            assertEquals(3.5, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("minus(Integer, Float) — mixed")
        void testMinusIntFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 3.5\n#->extend(~c: x | $x.a - $x.b)");
            assertEquals(6.5, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        // === times ===
        @Test @DisplayName("times(Integer, Integer)")
        void testTimesIntInt() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n6, 7\n#->extend(~c: x | $x.a * $x.b)");
            assertEquals(42L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("times(Float, Float)")
        void testTimesFloatFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n2.5, 4.0\n#->extend(~c: x | $x.a * $x.b)");
            assertEquals(10.0, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("times(Integer, Float) — mixed")
        void testTimesIntFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n3, 2.5\n#->extend(~c: x | $x.a * $x.b)");
            assertEquals(7.5, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }

        // === divide ===
        @Test @DisplayName("divide(Number, Number)")
        void testDivide() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 4\n#->extend(~c: x | $x.a / $x.b)");
            assertEquals(2L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("divide(Float, Float)")
        void testDivideFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n7.5, 2.5\n#->extend(~c: x | $x.a / $x.b)");
            assertEquals(3.0, ((Number) r.rows().get(0).get(colIdx(r, "c"))).doubleValue(), 0.001);
        }
    }

    // ========================================================================
    // 8. String functions — advanced
    // ========================================================================

    @Nested
    @DisplayName("extend — string advanced")
    class StringExtendAdvanced {

        @Test @DisplayName("ltrim()")
        void testLtrim() throws SQLException {
            var r = executeRelation("|#TDS\nstr\n   hello\n#->extend(~t: x | $x.str->ltrim())");
            assertEquals("hello", r.rows().get(0).get(colIdx(r, "t")));
        }

        @Test @DisplayName("rtrim()")
        void testRtrim() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello   \n#->extend(~t: x | $x.str->rtrim())");
            assertEquals("hello", r.rows().get(0).get(colIdx(r, "t")));
        }

        @Test @DisplayName("reverseString()")
        void testReverseString() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello\n#->extend(~rev: x | $x.str->reverseString())");
            assertEquals("olleh", r.rows().get(0).get(colIdx(r, "rev")));
        }

        @Test @DisplayName("ascii()")
        void testAscii() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nA\n#->extend(~code: x | $x.str->ascii())");
            assertEquals(65L, ((Number) r.rows().get(0).get(colIdx(r, "code"))).longValue());
        }

        @Test @DisplayName("char()")
        void testChar() throws SQLException {
            var r = executeRelation("|#TDS\ncode\n65\n#->extend(~ch: x | $x.code->char())");
            assertEquals("A", r.rows().get(0).get(colIdx(r, "ch")));
        }

        @Test @DisplayName("lpad()")
        void testLpad() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhi\n#->extend(~padded: x | $x.str->lpad(5, '0'))");
            assertEquals("000hi", r.rows().get(0).get(colIdx(r, "padded")));
        }

        @Test @DisplayName("rpad()")
        void testRpad() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhi\n#->extend(~padded: x | $x.str->rpad(5, '0'))");
            assertEquals("hi000", r.rows().get(0).get(colIdx(r, "padded")));
        }

        @Test @DisplayName("splitPart()")
        void testSplitPart() throws SQLException {
            var r = executeRelation("|#TDS\nstr\na-b-c\n#->extend(~p: x | $x.str->splitPart('-', 1))");
            assertEquals("b", r.rows().get(0).get(colIdx(r, "p")));
        }

        @Test @DisplayName("left()")
        void testLeft() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello world\n#->extend(~l: x | $x.str->left(5))");
            assertEquals("hello", r.rows().get(0).get(colIdx(r, "l")));
        }

        @Test @DisplayName("right()")
        void testRight() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello world\n#->extend(~r: x | $x.str->right(5))");
            assertEquals("world", r.rows().get(0).get(colIdx(r, "r")));
        }

        @Test @DisplayName("toUpperFirstCharacter()")
        void testToUpperFirstChar() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello\n#->extend(~u: x | $x.str->toUpperFirstCharacter())");
            assertEquals("Hello", r.rows().get(0).get(colIdx(r, "u")));
        }

        @Test @DisplayName("toLowerFirstCharacter()")
        void testToLowerFirstChar() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nHello\n#->extend(~l: x | $x.str->toLowerFirstCharacter())");
            assertEquals("hello", r.rows().get(0).get(colIdx(r, "l")));
        }

        @Test @DisplayName("encodeBase64()")
        void testEncodeBase64() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello\n#->extend(~enc: x | $x.str->encodeBase64())");
            assertEquals("aGVsbG8=", r.rows().get(0).get(colIdx(r, "enc")));
        }

        @Test @DisplayName("decodeBase64()")
        void testDecodeBase64() throws SQLException {
            var r = executeRelation("|#TDS\nstr\naGVsbG8=\n#->extend(~dec: x | $x.str->decodeBase64())");
            assertEquals("hello", r.rows().get(0).get(colIdx(r, "dec")));
        }

        @Test @DisplayName("levenshteinDistance()")
        void testLevenshteinDistance() throws SQLException {
            var r = executeRelation("|#TDS\na, b\nkitten, sitting\n#->extend(~d: x | $x.a->levenshteinDistance($x.b))");
            assertEquals(3L, ((Number) r.rows().get(0).get(colIdx(r, "d"))).longValue());
        }

        @Test @DisplayName("jaroWinklerSimilarity()")
        void testJaroWinkler() throws SQLException {
            var r = executeRelation("|#TDS\na, b\nmartha, marhta\n#->extend(~sim: x | $x.a->jaroWinklerSimilarity($x.b))");
            assertTrue(((Number) r.rows().get(0).get(colIdx(r, "sim"))).doubleValue() > 0.9);
        }

        @Test @DisplayName("matches()")
        void testMatches() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello123\nabc\n#->extend(~m: x | $x.str->matches('.*\\\\d+.*'))");
            int idx = colIdx(r, "m");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("hash() — MD5")
        void testHash() throws SQLException {
            var r = executeRelation("|#TDS\nstr\nhello\n#->extend(~h: x | $x.str->hash('MD5'))");
            // MD5 of "hello" = 5d41402abc4b2a76b9719d911017c592
            assertEquals("5d41402abc4b2a76b9719d911017c592",
                    r.rows().get(0).get(colIdx(r, "h")));
        }
    }

    // ========================================================================
    // 9. Conversion functions
    // ========================================================================

    @Nested
    @DisplayName("extend — conversions")
    class ConversionExtend {

        @Test @DisplayName("parseDecimal()")
        void testParseDecimal() throws SQLException {
            var r = executeRelation("|#TDS\nstr:String\n3.14159\n#->extend(~d: x | $x.str->parseDecimal())");
            assertEquals(3.14159, ((Number) r.rows().get(0).get(colIdx(r, "d"))).doubleValue(), 0.00001);
        }

        @Test @DisplayName("parseBoolean()")
        void testParseBoolean() throws SQLException {
            var r = executeRelation("|#TDS\nstr:String\ntrue\nfalse\n#->extend(~b: x | $x.str->parseBoolean())");
            int idx = colIdx(r, "b");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("toDecimal()")
        void testToDecimal() throws SQLException {
            var r = executeRelation("|#TDS\nval\n42\n#->extend(~d: x | $x.val->toDecimal())");
            assertEquals(42.0, ((Number) r.rows().get(0).get(colIdx(r, "d"))).doubleValue(), 0.001);
        }

        @Test @DisplayName("toFloat()")
        void testToFloat() throws SQLException {
            var r = executeRelation("|#TDS\nval\n42\n#->extend(~f: x | $x.val->toFloat())");
            assertEquals(42.0, ((Number) r.rows().get(0).get(colIdx(r, "f"))).doubleValue(), 0.001);
        }
    }

    // ========================================================================
    // 10. Comparison operators and functions
    // ========================================================================

    @Nested
    @DisplayName("extend — comparisons")
    class ComparisonExtend {

        @Test @DisplayName("greaterThan (>)")
        void testGreaterThan() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 5\n3, 7\n#->extend(~gt: x | $x.a > $x.b)");
            int idx = colIdx(r, "gt");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("lessThan (<)")
        void testLessThan() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n3, 7\n10, 5\n#->extend(~lt: x | $x.a < $x.b)");
            int idx = colIdx(r, "lt");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("greaterThanEqual (>=)")
        void testGte() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n5, 5\n3, 7\n#->extend(~gte: x | $x.a >= $x.b)");
            int idx = colIdx(r, "gte");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("lessThanEqual (<=)")
        void testLte() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n5, 5\n10, 3\n#->extend(~lte: x | $x.a <= $x.b)");
            int idx = colIdx(r, "lte");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("equal (==)")
        void testEqual() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n5, 5\n5, 3\n#->extend(~eq: x | $x.a == $x.b)");
            int idx = colIdx(r, "eq");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("equal with string")
        void testEqualString() throws SQLException {
            var r = executeRelation("|#TDS\nname\nAlice\nBob\n#->extend(~isAlice: x | $x.name == 'Alice')");
            int idx = colIdx(r, "isAlice");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("coalesce()")
        void testCoalesce() throws SQLException {
            var r = executeRelation("|#TDS\nval\n42\n#->extend(~c: x | $x.val->coalesce(0))");
            assertEquals(42L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("greatest()")
        void testGreatest() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n3, 7\n10, 2\n#->extend(~g: x | [$x.a, $x.b]->greatest())");
            int idx = colIdx(r, "g");
            assertEquals(7L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(10L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("least()")
        void testLeast() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n3, 7\n10, 2\n#->extend(~l: x | [$x.a, $x.b]->least())");
            int idx = colIdx(r, "l");
            assertEquals(3L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(2L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("compare — greaterThan with float columns")
        void testGreaterThanFloat() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n3.5, 2.1\n1.0, 4.0\n#->extend(~gt: x | $x.a > $x.b)");
            int idx = colIdx(r, "gt");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("compare — integer vs literal float")
        void testIntVsLiteral() throws SQLException {
            var r = executeRelation("|#TDS\nval\n5\n15\n#->extend(~big: x | $x.val > 10)");
            int idx = colIdx(r, "big");
            assertEquals(false, r.rows().get(0).get(idx));
            assertEquals(true, r.rows().get(1).get(idx));
        }
    }

    // ========================================================================
    // 11. Boolean logic
    // ========================================================================

    @Nested
    @DisplayName("extend — boolean logic")
    class BooleanExtend {

        @Test @DisplayName("and")
        void testAnd() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 20\n5, 3\n#->extend(~both: x | ($x.a > 3) && ($x.b > 10))");
            int idx = colIdx(r, "both");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("or")
        void testOr() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n1, 20\n1, 1\n#->extend(~either: x | ($x.a > 5) || ($x.b > 10))");
            int idx = colIdx(r, "either");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("not")
        void testNot() throws SQLException {
            var r = executeRelation("|#TDS\nval\n10\n20\n#->extend(~notBig: x | !($x.val > 15))");
            int idx = colIdx(r, "notBig");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("complex boolean: (a > 5) && !(b < 3)")
        void testComplexBoolean() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n10, 5\n10, 1\n2, 5\n#->extend(~ok: x | ($x.a > 5) && !($x.b < 3))");
            int idx = colIdx(r, "ok");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
            assertEquals(false, r.rows().get(2).get(idx));
        }
    }

    // ========================================================================
    // 12. Bitwise operations
    // ========================================================================

    @Nested
    @DisplayName("extend — bitwise")
    class BitwiseExtend {

        @Test @DisplayName("bitAnd")
        void testBitAnd() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n12, 10\n#->extend(~c: x | $x.a->bitAnd($x.b))");
            assertEquals(8L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("bitOr")
        void testBitOr() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n12, 10\n#->extend(~c: x | $x.a->bitOr($x.b))");
            assertEquals(14L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("bitXor")
        void testBitXor() throws SQLException {
            var r = executeRelation("|#TDS\na, b\n12, 10\n#->extend(~c: x | $x.a->bitXor($x.b))");
            assertEquals(6L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("bitShiftLeft")
        void testBitShiftLeft() throws SQLException {
            var r = executeRelation("|#TDS\nval\n1\n#->extend(~c: x | $x.val->bitShiftLeft(3))");
            assertEquals(8L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }

        @Test @DisplayName("bitShiftRight")
        void testBitShiftRight() throws SQLException {
            var r = executeRelation("|#TDS\nval\n8\n#->extend(~c: x | $x.val->bitShiftRight(3))");
            assertEquals(1L, ((Number) r.rows().get(0).get(colIdx(r, "c"))).longValue());
        }
    }

    // ========================================================================
    // 13. Conditional — if, coalesce
    // ========================================================================

    @Nested
    @DisplayName("extend — conditional")
    class ConditionalExtend {

        @Test @DisplayName("if-then-else")
        void testIf() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    #->extend(~label: x | if($x.val > 15, |'big', |'small'))""");
            int idx = colIdx(r, "label");
            assertEquals("small", r.rows().get(0).get(idx));
            assertEquals("big", r.rows().get(1).get(idx));
        }

        @Test @DisplayName("if with numeric result")
        void testIfNumeric() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    #->extend(~capped: x | if($x.val > 15, |15, |$x.val))""");
            int idx = colIdx(r, "capped");
            assertEquals(10L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(15L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("nested if")
        void testNestedIf() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    5
                    15
                    25
                    #->extend(~tier: x | if($x.val > 20, |'high', |if($x.val > 10, |'mid', |'low')))""");
            int idx = colIdx(r, "tier");
            assertEquals("low", r.rows().get(0).get(idx));
            assertEquals("mid", r.rows().get(1).get(idx));
            assertEquals("high", r.rows().get(2).get(idx));
        }
    }

    // ========================================================================
    // 14. Edge cases
    // ========================================================================

    @Nested
    @DisplayName("extend — edge cases")
    class EdgeCaseExtend {

        @Test @DisplayName("extend on empty TDS")
        void testEmptyTds() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val:Integer
                    #->extend(~doubled: x | $x.val * 2)""");
            assertEquals(0, r.rowCount());
            assertEquals(2, r.columns().size());
        }

        @Test @DisplayName("chained extend→extend")
        void testChainedExtend() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    5
                    #->extend(~doubled: x | $x.val * 2)->extend(~quadrupled: x | $x.doubled * 2)""");
            assertEquals(20L, ((Number) r.rows().get(0).get(colIdx(r, "quadrupled"))).longValue());
        }

        @Test @DisplayName("extend→filter pipeline")
        void testExtendThenFilter() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    5
                    15
                    25
                    #->extend(~big: x | $x.val > 10)->filter(x | $x.big == true)""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→sort pipeline")
        void testExtendThenSort() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    3
                    1
                    2
                    #->extend(~neg: x | 0 - $x.val)->sort(descending(~neg))""");
            assertEquals(-1L, ((Number) r.rows().get(0).get(colIdx(r, "neg"))).longValue());
            assertEquals(-2L, ((Number) r.rows().get(1).get(colIdx(r, "neg"))).longValue());
            assertEquals(-3L, ((Number) r.rows().get(2).get(colIdx(r, "neg"))).longValue());
        }

        @Test @DisplayName("filter→extend pipeline")
        void testFilterThenExtend() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    5
                    15
                    25
                    #->filter(x | $x.val > 10)->extend(~doubled: x | $x.val * 2)""");
            assertEquals(2, r.rowCount());
            assertEquals(30L, ((Number) r.rows().get(0).get(colIdx(r, "doubled"))).longValue());
            assertEquals(50L, ((Number) r.rows().get(1).get(colIdx(r, "doubled"))).longValue());
        }

        @Test @DisplayName("extend with many columns (5 columns)")
        void testManyColumns() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    v
                    10
                    #->extend(~[a: x | $x.v + 1, b: x | $x.v + 2, c: x | $x.v + 3, d: x | $x.v + 4, e: x | $x.v + 5])""");
            assertEquals(6, r.columns().size());
            assertEquals(15L, ((Number) r.rows().get(0).get(colIdx(r, "e"))).longValue());
        }

        @Test @DisplayName("extend duplicate column throws — array syntax")
        void testDuplicateInArray() {
            assertThrows(com.gs.legend.compiler.PureCompileException.class, () ->
                executeRelation("""
                    |#TDS
                    val
                    1
                    #->extend(~[dup: x | $x.val, dup: x | $x.val + 1])"""));
        }

        @Test @DisplayName("extend references nonexistent column throws")
        void testNonexistentColumn() {
            assertThrows(Exception.class, () ->
                executeRelation("""
                    |#TDS
                    val
                    1
                    #->extend(~bad: x | $x.nonexistent + 1)"""));
        }
    }

    // ========================================================================
    // 15. Null handling — isEmpty, isNotEmpty
    // ========================================================================

    @Nested
    @DisplayName("extend — null handling")
    class NullHandlingExtend {

        @Test @DisplayName("isEmpty on nullable column")
        void testIsEmpty() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->extend(~missing: x | $x.name->isEmpty())""");
            int idx = colIdx(r, "missing");
            assertEquals(false, r.rows().get(0).get(idx));
        }

        @Test @DisplayName("isNotEmpty on nullable column")
        void testIsNotEmpty() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    #->extend(~present: x | $x.name->isNotEmpty())""");
            assertEquals(true, r.rows().get(0).get(colIdx(r, "present")));
        }

        @Test @DisplayName("toOne on column")
        void testToOne() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    42
                    #->extend(~v: x | $x.val->toOne())""");
            assertEquals(42L, ((Number) r.rows().get(0).get(colIdx(r, "v"))).longValue());
        }
    }

    // ========================================================================
    // 16. Pipeline chaining — extend → other operations
    // ========================================================================

    @Nested
    @DisplayName("extend — pipeline chaining")
    class PipelineExtend {

        @Test @DisplayName("extend→select")
        void testExtendThenSelect() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, val
                    1, 10
                    2, 20
                    #->extend(~doubled: x | $x.val * 2)->select(~[id, doubled])""");
            assertEquals(2, r.columns().size());
            assertEquals("id", r.columns().get(0).name());
            assertEquals("doubled", r.columns().get(1).name());
        }

        @Test @DisplayName("extend→distinct")
        void testExtendThenDistinct() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    10
                    20
                    #->extend(~doubled: x | $x.val * 2)->distinct(~doubled)""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→groupBy with agg")
        void testExtendThenGroupBy() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->extend(~doubled: x | $x.val * 2)
                     ->groupBy(~grp, ~total:x|$x.doubled:y|$y->plus())""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→limit")
        void testExtendThenLimit() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    #->extend(~doubled: x | $x.val * 2)->limit(2)""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→drop")
        void testExtendThenDrop() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    #->extend(~doubled: x | $x.val * 2)->drop(1)""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→slice")
        void testExtendThenSlice() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    40
                    #->extend(~doubled: x | $x.val * 2)->slice(1, 3)""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→rename")
        void testExtendThenRename() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    #->extend(~doubled: x | $x.val * 2)->rename(~doubled, ~result)""");
            assertEquals(2, r.columns().size());
            assertEquals("result", r.columns().get(1).name());
        }

        @Test @DisplayName("extend→concatenate")
        void testExtendThenConcatenate() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    10
                    #->extend(~doubled: x | $x.val * 2)->concatenate(
                    |#TDS
                    val, doubled
                    20, 40
                    #)""");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("extend→sort→limit (top-N pattern)")
        void testExtendSortLimit() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    1
                    3
                    2
                    5
                    4
                    #->extend(~sq: x | $x.val * $x.val)
                     ->sort(descending(~sq))
                     ->limit(3)""");
            assertEquals(3, r.rowCount());
            assertEquals(25L, ((Number) r.rows().get(0).get(colIdx(r, "sq"))).longValue());
        }
    }

    // ========================================================================
    // 17. Multi-step chained extends — 3+ referencing prior computed columns
    // ========================================================================

    @Nested
    @DisplayName("extend — multi-step chains")
    class MultiStepExtend {

        @Test @DisplayName("3 chained extends: val → doubled → quadrupled")
        void testTripleChain() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    val
                    5
                    #->extend(~doubled: x | $x.val * 2)
                     ->extend(~quadrupled: x | $x.doubled * 2)
                     ->extend(~octupled: x | $x.quadrupled * 2)""");
            assertEquals(4, r.columns().size());
            assertEquals(40L, ((Number) r.rows().get(0).get(colIdx(r, "octupled"))).longValue());
        }

        @Test @DisplayName("extend→extend using multiple prior columns")
        void testMultiplePriorColumns() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    a, b
                    3, 4
                    #->extend(~sum: x | $x.a + $x.b)
                     ->extend(~prod: x | $x.a * $x.b)
                     ->extend(~ratio: x | $x.sum * $x.prod)""");
            assertEquals(5, r.columns().size());
            assertEquals(84L, ((Number) r.rows().get(0).get(colIdx(r, "ratio"))).longValue());
        }

        @Test @DisplayName("extend string then extend using string result")
        void testStringChained() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    first, last
                    Alice, Smith
                    #->extend(~full: x | $x.first + ' ' + $x.last)
                     ->extend(~upper: x | $x.full->toUpper())""");
            assertEquals("ALICE SMITH", r.rows().get(0).get(colIdx(r, "upper")));
        }
    }

    // ========================================================================
    // 18. Boolean source columns in extend
    // ========================================================================

    @Nested
    @DisplayName("extend — boolean source columns")
    class BooleanSourceExtend {

        @Test @DisplayName("extend from boolean column with not")
        void testBooleanColumnNot() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    active:Boolean
                    true
                    false
                    #->extend(~inactive: x | !$x.active)""");
            int idx = colIdx(r, "inactive");
            assertEquals(false, r.rows().get(0).get(idx));
            assertEquals(true, r.rows().get(1).get(idx));
        }

        @Test @DisplayName("extend from boolean column with and")
        void testBooleanColumnAnd() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    a:Boolean, b:Boolean
                    true, true
                    true, false
                    false, false
                    #->extend(~both: x | $x.a && $x.b)""");
            int idx = colIdx(r, "both");
            assertEquals(true, r.rows().get(0).get(idx));
            assertEquals(false, r.rows().get(1).get(idx));
            assertEquals(false, r.rows().get(2).get(idx));
        }

        @Test @DisplayName("if based on boolean column")
        void testIfBooleanColumn() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    active:Boolean, name
                    true, Alice
                    false, Bob
                    #->extend(~label: x | if($x.active, |'active', |'inactive'))""");
            int idx = colIdx(r, "label");
            assertEquals("active", r.rows().get(0).get(idx));
            assertEquals("inactive", r.rows().get(1).get(idx));
        }
    }

    // ========================================================================
    // 14. Date/Time functions in extend
    // ========================================================================

    @Nested
    @DisplayName("extend — date/time functions")
    class DateTimeExtend {

        @Test @DisplayName("year() — extract year from date literal")
        void testYear() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:StrictDate
                    1, %2024-06-15
                    2, %2023-01-01
                    #->extend(~yr: x | $x.dt->year())""");
            int idx = colIdx(r, "yr");
            assertEquals(2024L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(2023L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("monthNumber() — extract month")
        void testMonthNumber() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:StrictDate
                    1, %2024-06-15
                    2, %2024-12-25
                    #->extend(~mon: x | $x.dt->monthNumber())""");
            int idx = colIdx(r, "mon");
            assertEquals(6L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(12L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("dayOfMonth() — extract day")
        void testDayOfMonth() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:StrictDate
                    1, %2024-06-15
                    2, %2024-12-25
                    #->extend(~d: x | $x.dt->dayOfMonth())""");
            int idx = colIdx(r, "d");
            assertEquals(15L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(25L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("hour() — extract hour from datetime")
        void testHour() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:DateTime
                    1, %2024-06-15T10:30:00
                    2, %2024-06-15T23:45:00
                    #->extend(~h: x | $x.dt->hour())""");
            int idx = colIdx(r, "h");
            assertEquals(10L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(23L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("minute() — extract minute from datetime")
        void testMinute() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:DateTime
                    1, %2024-06-15T10:30:00
                    2, %2024-06-15T23:45:00
                    #->extend(~m: x | $x.dt->minute())""");
            int idx = colIdx(r, "m");
            assertEquals(30L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(45L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("second() — extract second from datetime")
        void testSecond() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:DateTime
                    1, %2024-06-15T10:30:45
                    2, %2024-06-15T23:45:15
                    #->extend(~s: x | $x.dt->second())""");
            int idx = colIdx(r, "s");
            assertEquals(45L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(15L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("dateDiff() — days between dates")
        void testDateDiff() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, d1:StrictDate, d2:StrictDate
                    1, %2024-01-01, %2024-01-31
                    2, %2024-06-01, %2024-06-15
                    #->extend(~diff: x | dateDiff($x.d1, $x.d2, meta::pure::functions::date::DurationUnit.DAYS))""");
            int idx = colIdx(r, "diff");
            assertEquals(30L, ((Number) r.rows().get(0).get(idx)).longValue());
            assertEquals(14L, ((Number) r.rows().get(1).get(idx)).longValue());
        }

        @Test @DisplayName("adjust() — add days to date")
        void testAdjust() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, d:StrictDate
                    1, %2024-01-15
                    #->extend(~adjusted: x | $x.d->meta::pure::functions::date::adjust(10, meta::pure::functions::date::DurationUnit.DAYS))""");
            assertEquals(1, r.rowCount());
            // 2024-01-15 + 10 days = 2024-01-25
            String adjusted = r.rows().get(0).get(colIdx(r, "adjusted")).toString();
            assertTrue(adjusted.contains("2024-01-25"), "Expected 2024-01-25 but got: " + adjusted);
        }

        @Test @DisplayName("today() — current date literal in extend")
        void testToday() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id
                    1
                    #->extend(~td: x | today())""");
            assertEquals(1, r.rowCount());
            // today() should return a date containing the current year
            String td = r.rows().get(0).get(colIdx(r, "td")).toString();
            assertTrue(td.startsWith(String.valueOf(java.time.Year.now().getValue())), "today() should start with current year");
        }

        @Test @DisplayName("now() — current datetime in extend")
        void testNow() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id
                    1
                    #->extend(~ts: x | now())""");
            assertEquals(1, r.rowCount());
            // now() should return a datetime containing the current year
            String ts = r.rows().get(0).get(colIdx(r, "ts")).toString();
            assertTrue(ts.startsWith(String.valueOf(java.time.Year.now().getValue())), "now() should start with current year");
        }

        @Test @DisplayName("datePart() — truncate datetime to date")
        void testDatePart() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:DateTime
                    1, %2024-06-15T10:30:00
                    #->extend(~dp: x | $x.dt->datePart())""");
            assertEquals(1, r.rowCount());
            // datePart(2024-06-15T10:30:00) should yield 2024-06-15
            String dp = r.rows().get(0).get(colIdx(r, "dp")).toString();
            assertTrue(dp.contains("2024-06-15"), "datePart should extract 2024-06-15 but got: " + dp);
        }

        @Test @DisplayName("hasMonth/hasDay/hasHour/hasMinute/hasSecond — date part checks")
        void testHasDateParts() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:DateTime
                    1, %2024-06-15T10:30:45
                    #->extend(~[hasM: x | $x.dt->meta::pure::functions::date::hasMonth(), hasD: x | $x.dt->meta::pure::functions::date::hasDay(), hasH: x | $x.dt->meta::pure::functions::date::hasHour(), hasMi: x | $x.dt->meta::pure::functions::date::hasMinute(), hasS: x | $x.dt->meta::pure::functions::date::hasSecond()])""");
            assertEquals(1, r.rowCount());
            // DuckDB returns integers for has* functions (1 = true, 0 = false)
            assertTrue(((Number) r.rows().get(0).get(colIdx(r, "hasM"))).intValue() != 0);
            assertTrue(((Number) r.rows().get(0).get(colIdx(r, "hasD"))).intValue() != 0);
            assertTrue(((Number) r.rows().get(0).get(colIdx(r, "hasH"))).intValue() != 0);
            assertTrue(((Number) r.rows().get(0).get(colIdx(r, "hasMi"))).intValue() != 0);
            assertTrue(((Number) r.rows().get(0).get(colIdx(r, "hasS"))).intValue() != 0);
        }

        @Test @DisplayName("combined: year + month + day in same extend")
        void testCombinedDateParts() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:StrictDate
                    1, %2024-06-15
                    #->extend(~[yr: x | $x.dt->year(), mon: x | $x.dt->monthNumber(), day: x | $x.dt->dayOfMonth()])""");
            assertEquals(1, r.rowCount());
            assertEquals(2024L, ((Number) r.rows().get(0).get(colIdx(r, "yr"))).longValue());
            assertEquals(6L, ((Number) r.rows().get(0).get(colIdx(r, "mon"))).longValue());
            assertEquals(15L, ((Number) r.rows().get(0).get(colIdx(r, "day"))).longValue());
        }

        @Test @DisplayName("date() — construct date from year/month/day")
        void testDateConstructor() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, yr, mon, day
                    1, 2024, 6, 15
                    #->extend(~constructed: x | date($x.yr, $x.mon, $x.day))""");
            assertEquals(1, r.rowCount());
            // date(2024, 6, 15) should produce 2024-06-15
            String constructed = r.rows().get(0).get(colIdx(r, "constructed")).toString();
            assertTrue(constructed.contains("2024-06-15"), "Constructed date should be 2024-06-15 but got: " + constructed);
        }

        @Test @DisplayName("timeBucket() — bucket date by days")
        void testTimeBucket() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dt:StrictDate
                    1, %2024-01-05
                    2, %2024-01-12
                    3, %2024-01-20
                    #->extend(~bucket: x | timeBucket($x.dt, 7, meta::pure::functions::date::DurationUnit.DAYS))""");
            assertEquals(3, r.rowCount());
            // timeBucket of 2024-01-05 with 7-day bucket should produce a date
            String bucket = r.rows().get(0).get(colIdx(r, "bucket")).toString();
            assertTrue(bucket.contains("2024-01"), "Bucket should be in January 2024 but got: " + bucket);
        }

        @Test @DisplayName("parseDate() — parse string to date")
        void testParseDate() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, dateStr
                    1, 2024-06-15
                    #->extend(~parsed: x | $x.dateStr->parseDate())""");
            assertEquals(1, r.rowCount());
            // parseDate("2024-06-15") should produce a date containing 2024-06-15
            String parsed = r.rows().get(0).get(colIdx(r, "parsed")).toString();
            assertTrue(parsed.contains("2024-06-15"), "Parsed date should be 2024-06-15 but got: " + parsed);
        }
    }

    // ========================================================================
    // 15. Bivariant RowMapper functions in extend (fn1+fn2 pattern)
    // ========================================================================

    @Nested
    @DisplayName("extend — bivariant RowMapper (fn1+fn2)")
    class BivariantExtend {

        @Test @DisplayName("wavg — weighted average over full relation")
        void testWavg() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    id, quantity, weight
                    1, 100, 0.5
                    2, 200, 0.5
                    #->extend(~wa:x|meta::pure::functions::math::mathUtility::rowMapper($x.quantity, $x.weight):y|$y->wavg())""");
            assertEquals(2, r.rowCount());
            // wavg = (100*0.5 + 200*0.5) / (0.5+0.5) = 150.0 — same for all rows (no-window)
            int idx = colIdx(r, "wa");
            assertEquals(150.0, ((Number) r.rows().get(0).get(idx)).doubleValue(), 0.01);
            assertEquals(150.0, ((Number) r.rows().get(1).get(idx)).doubleValue(), 0.01);
        }

        @Test @DisplayName("maxBy — value column with max of sort column")
        void testMaxBy() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    name, score
                    Alice, 85
                    Bob, 92
                    Charlie, 78
                    #->extend(~winner:x|meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.score):y|$y->maxBy())""");
            assertEquals(3, r.rowCount());
            int idx = colIdx(r, "winner");
            // maxBy returns the name of the row with the highest score ("Bob" = 92)
            assertEquals("Bob", r.rows().get(0).get(idx).toString());
        }

        @Test @DisplayName("minBy — value column with min of sort column")
        void testMinBy() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    name, score
                    Alice, 85
                    Bob, 92
                    Charlie, 78
                    #->extend(~loser:x|meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.score):y|$y->minBy())""");
            assertEquals(3, r.rowCount());
            int idx = colIdx(r, "loser");
            assertEquals("Charlie", r.rows().get(0).get(idx).toString());
        }

        @Test @DisplayName("corr — correlation coefficient")
        void testCorr() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    a, b
                    1, 10
                    2, 20
                    3, 30
                    #->extend(~c:x|meta::pure::functions::math::mathUtility::rowMapper($x.a, $x.b):y|$y->corr())""");
            assertEquals(3, r.rowCount());
            int idx = colIdx(r, "c");
            // Perfect positive correlation
            assertEquals(1.0, ((Number) r.rows().get(0).get(idx)).doubleValue(), 0.0001);
        }

        @Test @DisplayName("covarSample — sample covariance")
        void testCovarSample() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    a, b
                    1, 10
                    2, 20
                    3, 30
                    #->extend(~cs:x|meta::pure::functions::math::mathUtility::rowMapper($x.a, $x.b):y|$y->covarSample())""");
            assertEquals(3, r.rowCount());
            int idx = colIdx(r, "cs");
            // covarSample of [1,2,3] and [10,20,30] = 10.0
            assertEquals(10.0, ((Number) r.rows().get(0).get(idx)).doubleValue(), 0.01);
        }

        @Test @DisplayName("covarPopulation — population covariance")
        void testCovarPopulation() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    a, b
                    1, 10
                    2, 20
                    3, 30
                    #->extend(~cp:x|meta::pure::functions::math::mathUtility::rowMapper($x.a, $x.b):y|$y->covarPopulation())""");
            assertEquals(3, r.rowCount());
            int idx = colIdx(r, "cp");
            // covarPopulation of [1,2,3] and [10,20,30] ≈ 6.667
            assertEquals(6.667, ((Number) r.rows().get(0).get(idx)).doubleValue(), 0.01);
        }

        @Test @DisplayName("wavg with partition — weighted average per group")
        void testWavgWithPartition() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    grp, qty, wt
                    1, 100, 0.5
                    1, 200, 0.5
                    2, 300, 1.0
                    #->extend(over(~grp), ~wa:{p,w,r|meta::pure::functions::math::mathUtility::rowMapper($r.qty, $r.wt)}:y|$y->wavg())""");
            assertEquals(3, r.rowCount());
        }
    }

    // ========================================================================
    // 16. Miscellaneous scalar functions
    // ========================================================================

    @Nested
    @DisplayName("extend — misc scalar functions")
    class MiscScalarExtend {

        @Test @DisplayName("xor — exclusive or")
        void testXor() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                    a:Boolean, b:Boolean
                    true, false
                    true, true
                    false, false
                    #->extend(~xr: x | $x.a->xor($x.b))""");
            int idx = colIdx(r, "xr");
            assertEquals(true, r.rows().get(0).get(idx));   // true XOR false = true
            assertEquals(false, r.rows().get(1).get(idx));  // true XOR true = false
            assertEquals(false, r.rows().get(2).get(idx));  // false XOR false = false
        }
    }

    // ========================================================================
    // Utilities
    // ========================================================================

    /** Finds column index by name. */
    private int colIdx(ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name())) return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }

    /** Collects results into a map keyed by the specified column value. */
    private <K> Map<K, Object> collectResults(
            ExecutionResult result, String keyCol, String valCol) {
        int keyIdx = colIdx(result, keyCol);
        int valIdx = colIdx(result, valCol);
        Map<K, Object> map = new HashMap<>();
        for (var row : result.rows()) {
            @SuppressWarnings("unchecked")
            K key = (K) row.get(keyIdx);
            map.put(key, row.get(valIdx));
        }
        return map;
    }
}
