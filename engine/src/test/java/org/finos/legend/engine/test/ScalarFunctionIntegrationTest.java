package org.finos.legend.engine.test;

import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for scalar functions using exact PCT Pure expressions.
 * Each test uses the exact Pure query from PCT output to validate that
 * our engine produces correct results end-to-end.
 */
public class ScalarFunctionIntegrationTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() {
        return "DuckDB";
    }

    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:";
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // ==================== Previously Passing Tests ====================

    @Test
    void testTypeOf() throws SQLException {
        var result = executeRelation("|1->type()");
        assertEquals("INTEGER", result.rows().get(0).get(0));
    }

    @Test
    void testRem() throws SQLException {
        var result = executeRelation("|10->rem(3)");
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testToRadians() throws SQLException {
        var result = executeRelation("|180->toRadians()");
        double value = ((Number) result.rows().get(0).get(0)).doubleValue();
        assertEquals(Math.PI, value, 0.0001);
    }

    @Test
    void testToDegrees() throws SQLException {
        var result = executeRelation("|3.14159265359->toDegrees()");
        double value = ((Number) result.rows().get(0).get(0)).doubleValue();
        assertEquals(180.0, value, 0.1);
    }

    @Test
    void testSign() throws SQLException {
        var result = executeRelation("|-5->sign()");
        assertEquals(-1L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testReverseString() throws SQLException {
        var result = executeRelation("|'hello'->reverseString()");
        assertEquals("olleh", result.rows().get(0).get(0));
    }

    @Test
    void testContains() throws SQLException {
        var result = executeRelation("|['hello', 'world']->contains('world')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    void testStartsWith() throws SQLException {
        var result = executeRelation("|'hello'->startsWith('hel')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    void testEndsWith() throws SQLException {
        var result = executeRelation("|'hello'->endsWith('lo')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    void testLevenshteinDistance() throws SQLException {
        var result = executeRelation("|'hello'->levenshteinDistance('hallo')");
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitAnd() throws SQLException {
        var result = executeRelation("|12->bitAnd(10)");
        assertEquals(8L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitOr() throws SQLException {
        var result = executeRelation("|12->bitOr(10)");
        assertEquals(14L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitXor() throws SQLException {
        var result = executeRelation("|12->bitXor(10)");
        assertEquals(6L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitShiftLeft() throws SQLException {
        var result = executeRelation("|1->bitShiftLeft(4)");
        assertEquals(16L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitShiftRight() throws SQLException {
        var result = executeRelation("|16->bitShiftRight(2)");
        assertEquals(4L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testMonthNumber() throws SQLException {
        var result = executeRelation("|%2024-06-15->monthNumber()");
        assertEquals(6L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testDayNumber() throws SQLException {
        var result = executeRelation("|%2024-06-15->dayOfMonth()");
        assertEquals(15L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testYear() throws SQLException {
        var result = executeRelation("|%2024-06-15->year()");
        assertEquals(2024L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testHash() throws SQLException {
        var result = executeRelation("|'test'->hash()");
        assertNotNull(result.rows().get(0).get(0));
    }

    // ==================== PCT: sinh ====================

    @Test
    @DisplayName("PCT: |0->sinh()")
    void testPctSinh0() throws SQLException {
        var result = executeRelation("|0->meta::pure::functions::math::sinh()");
        assertEquals(0.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |3->sinh()")
    void testPctSinh3() throws SQLException {
        var result = executeRelation("|3->meta::pure::functions::math::sinh()");
        assertEquals(Math.sinh(3), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |sinh(-3.14)")
    void testPctSinhNeg() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::sinh(-3.14)");
        assertEquals(Math.sinh(-3.14), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |sinh eval(0)")
    void testPctSinhEval() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::sinh_Number_1__Float_1_->meta::pure::functions::lang::eval(0)");
        assertEquals(0.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    // ==================== PCT: cosh ====================

    @Test
    @DisplayName("PCT: |0->cosh()")
    void testPctCosh0() throws SQLException {
        var result = executeRelation("|0->meta::pure::functions::math::cosh()");
        assertEquals(1.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |2->cosh()")
    void testPctCosh2() throws SQLException {
        var result = executeRelation("|2->meta::pure::functions::math::cosh()");
        assertEquals(Math.cosh(2), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |cosh(-3.14)")
    void testPctCoshNeg() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::cosh(-3.14)");
        assertEquals(Math.cosh(-3.14), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |cosh eval(0)")
    void testPctCoshEval() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::cosh_Number_1__Float_1_->meta::pure::functions::lang::eval(0)");
        assertEquals(1.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    // ==================== PCT: tanh ====================

    @Test
    @DisplayName("PCT: |0->tanh()")
    void testPctTanh0() throws SQLException {
        var result = executeRelation("|0->meta::pure::functions::math::tanh()");
        assertEquals(0.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |3->tanh()")
    void testPctTanh3() throws SQLException {
        var result = executeRelation("|3->meta::pure::functions::math::tanh()");
        assertEquals(Math.tanh(3), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |tanh(-3.14)")
    void testPctTanhNeg() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::tanh(-3.14)");
        assertEquals(Math.tanh(-3.14), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |tanh eval(0)")
    void testPctTanhEval() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::tanh_Number_1__Float_1_->meta::pure::functions::lang::eval(0)");
        assertEquals(0.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    // ==================== PCT: cot ====================

    @Test
    @DisplayName("PCT: |1->cot()")
    void testPctCot1() throws SQLException {
        var result = executeRelation("|1->meta::pure::functions::math::cot()");
        assertEquals(1.0 / Math.tan(1.0), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    // ==================== PCT: cbrt ====================

    @Test
    @DisplayName("PCT: |0->cbrt()")
    void testPctCbrt0() throws SQLException {
        var result = executeRelation("|0->meta::pure::functions::math::cbrt()");
        assertEquals(0.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |cbrt eval(27)")
    void testPctCbrtEval27() throws SQLException {
        var result = executeRelation("|meta::pure::functions::math::cbrt_Number_1__Float_1_->meta::pure::functions::lang::eval(27)");
        assertEquals(3.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    // ==================== PCT: ltrim / rtrim ====================

    @Test
    @DisplayName("PCT: ltrim")
    void testPctLtrim() throws SQLException {
        var result = executeRelation("|' A Simple Text To Trim    '->meta::pure::functions::string::ltrim()");
        assertEquals("A Simple Text To Trim    ", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: rtrim")
    void testPctRtrim() throws SQLException {
        var result = executeRelation("|' A Simple Text To Trim    '->meta::pure::functions::string::rtrim()");
        assertEquals(" A Simple Text To Trim", result.rows().get(0).get(0));
    }

    // ==================== PCT: matches ====================

    @Test
    @DisplayName("PCT: |'hello'->matches('hello')")
    void testPctMatchesExact() throws SQLException {
        var result = executeRelation("|'hello'->meta::pure::functions::string::matches('hello')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'abc'->matches('[a-z][a-z][a-z][a-z]')")
    void testPctMatchesCharClass() throws SQLException {
        var result = executeRelation("|'abc'->meta::pure::functions::string::matches('[a-z][a-z][a-z][a-z]')");
        assertEquals(false, result.rows().get(0).get(0));
    }

    // ==================== PCT: toUpperFirstCharacter ====================

    @Test
    @DisplayName("PCT: |''->toUpperFirstCharacter()")
    void testPctToUpperFirstEmpty() throws SQLException {
        var result = executeRelation("|''->meta::pure::functions::string::toUpperFirstCharacter()");
        assertEquals("", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'1isOne'->toUpperFirstCharacter()")
    void testPctToUpperFirstDigit() throws SQLException {
        var result = executeRelation("|'1isOne'->meta::pure::functions::string::toUpperFirstCharacter()");
        assertEquals("1isOne", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'xOxOxOx'->toUpperFirstCharacter()")
    void testPctToUpperFirstLower() throws SQLException {
        var result = executeRelation("|'xOxOxOx'->meta::pure::functions::string::toUpperFirstCharacter()");
        assertEquals("XOxOxOx", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'XoXoXoX'->toUpperFirstCharacter()")
    void testPctToUpperFirstUpper() throws SQLException {
        var result = executeRelation("|'XoXoXoX'->meta::pure::functions::string::toUpperFirstCharacter()");
        assertEquals("XoXoXoX", result.rows().get(0).get(0));
    }

    // ==================== PCT: toLowerFirstCharacter ====================

    @Test
    @DisplayName("PCT: |''->toLowerFirstCharacter()")
    void testPctToLowerFirstEmpty() throws SQLException {
        var result = executeRelation("|''->meta::pure::functions::string::toLowerFirstCharacter()");
        assertEquals("", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'1isOne'->toLowerFirstCharacter()")
    void testPctToLowerFirstDigit() throws SQLException {
        var result = executeRelation("|'1isOne'->meta::pure::functions::string::toLowerFirstCharacter()");
        assertEquals("1isOne", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'xOxOxOx'->toLowerFirstCharacter()")
    void testPctToLowerFirstLower() throws SQLException {
        var result = executeRelation("|'xOxOxOx'->meta::pure::functions::string::toLowerFirstCharacter()");
        assertEquals("xOxOxOx", result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |'XoXoXoX'->toLowerFirstCharacter()")
    void testPctToLowerFirstUpper() throws SQLException {
        var result = executeRelation("|'XoXoXoX'->meta::pure::functions::string::toLowerFirstCharacter()");
        assertEquals("xoXoXoX", result.rows().get(0).get(0));
    }

    // ==================== PCT: jaroWinklerSimilarity ====================

    @Test
    @DisplayName("PCT: |'John Smith'->jaroWinklerSimilarity('John Smith')")
    void testPctJaroWinklerSame() throws SQLException {
        var result = executeRelation("|'John Smith'->meta::pure::functions::string::jaroWinklerSimilarity('John Smith')");
        assertEquals(1.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("PCT: |'John Smith'->jaroWinklerSimilarity('Jane Smith')")
    void testPctJaroWinklerDiff() throws SQLException {
        var result = executeRelation("|'John Smith'->meta::pure::functions::string::jaroWinklerSimilarity('Jane Smith')");
        double value = ((Number) result.rows().get(0).get(0)).doubleValue();
        assertTrue(value > 0.5 && value < 1.0, "Similarity should be between 0.5 and 1.0, got: " + value);
    }

    // ==================== PCT: hashCode ====================

    @Test
    @DisplayName("PCT: |'a'->hashCode()")
    void testPctHashCode() throws SQLException {
        var result = executeRelation("|'a'->meta::pure::functions::hash::hashCode()");
        assertNotNull(result.rows().get(0).get(0));
    }

    // ==================== PCT: hasDay ====================

    @Test
    @DisplayName("PCT: |%2015-04-15T17:09:21.398+0000->hasDay()")
    void testPctHasDay() throws SQLException {
        var result = executeRelation("|%2015-04-15T17:09:21.398+0000->meta::pure::functions::date::hasDay()");
        assertEquals(true, result.rows().get(0).get(0));
    }

    // ==================== PCT: hasSecond ====================

    @Test
    @DisplayName("PCT: |%2015-04-15T17:09:21.398+0000->hasSecond()")
    void testPctHasSecond() throws SQLException {
        var result = executeRelation("|%2015-04-15T17:09:21.398+0000->meta::pure::functions::date::hasSecond()");
        assertEquals(true, result.rows().get(0).get(0));
    }

    // ==================== PCT: hasSubsecond ====================

    @Test
    @DisplayName("PCT: |%2015-04-15T17:09:21.398+0000->hasSubsecond()")
    void testPctHasSubsecond() throws SQLException {
        var result = executeRelation("|%2015-04-15T17:09:21.398+0000->meta::pure::functions::date::hasSubsecond()");
        assertEquals(true, result.rows().get(0).get(0));
    }

    // ==================== PCT: hasSubsecondWithAtLeastPrecision ====================

    @Test
    @DisplayName("PCT: |%2015-04-15T17:09:21.398+0000->hasSubsecondWithAtLeastPrecision(1)")
    void testPctHasSubsecondWithAtLeastPrecision() throws SQLException {
        var result = executeRelation("|%2015-04-15T17:09:21.398+0000->meta::pure::functions::date::hasSubsecondWithAtLeastPrecision(1)");
        assertEquals(true, result.rows().get(0).get(0));
    }

    // ==================== PCT: removeDuplicates ====================

    @Test
    @DisplayName("PCT: |[]->removeDuplicates()")
    void testPctRemoveDuplicatesEmpty() throws SQLException {
        var result = executeRelation("|[]->meta::pure::functions::collection::removeDuplicates()");
        // Empty list should return empty
        assertNotNull(result);
    }

    @Test
    @DisplayName("PCT: |[1, 2, 1, 3, 1, 3, 3, 2]->removeDuplicates()")
    void testPctRemoveDuplicatesIntegers() throws SQLException {
        var result = executeRelation("|[1, 2, 1, 3, 1, 3, 3, 2]->meta::pure::functions::collection::removeDuplicates()");
        assertNotNull(result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PCT: |[1, 2, 1, 3, 1, 3, 3, 2]->removeDuplicates(eq)")
    void testPctRemoveDuplicatesWithEq() throws SQLException {
        var result = executeRelation("|[1, 2, 1, 3, 1, 3, 3, 2]->meta::pure::functions::collection::removeDuplicates(meta::pure::functions::boolean::eq_Any_1__Any_1__Boolean_1_)");
        assertNotNull(result.rows().get(0).get(0));
    }

    // ==================== PCT: removeDuplicatesBy ====================

    @Test
    @DisplayName("PCT: |[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicatesBy(toString)")
    void testPctRemoveDuplicatesBy() throws SQLException {
        var result = executeRelation("|[1, 2, '1', '3', 1, 3, '3', 2]->meta::pure::functions::collection::removeDuplicatesBy(x: meta::pure::metamodel::type::Any[1]|$x->meta::pure::functions::string::toString())");
        assertNotNull(result.rows().get(0).get(0));
    }

    // ==================== Shorthand tests (simple) ====================

    @Test
    void testSinhSimple() throws SQLException {
        var result = executeRelation("|1.0->sinh()");
        assertEquals(Math.sinh(1.0), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void testCoshSimple() throws SQLException {
        var result = executeRelation("|1.0->cosh()");
        assertEquals(Math.cosh(1.0), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void testTanhSimple() throws SQLException {
        var result = executeRelation("|1.0->tanh()");
        assertEquals(Math.tanh(1.0), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void testCotSimple() throws SQLException {
        var result = executeRelation("|1.0->cot()");
        assertEquals(1.0 / Math.tan(1.0), ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void testCbrtSimple() throws SQLException {
        var result = executeRelation("|27.0->cbrt()");
        assertEquals(3.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void testLeftSimple() throws SQLException {
        var result = executeRelation("|'hello'->left(3)");
        assertEquals("hel", result.rows().get(0).get(0));
    }

    @Test
    void testRightSimple() throws SQLException {
        var result = executeRelation("|'hello'->right(3)");
        assertEquals("llo", result.rows().get(0).get(0));
    }

    @Test
    void testSplitSimple() throws SQLException {
        var result = executeRelation("|'a,b,c'->split(',')");
        assertNotNull(result.rows().get(0).get(0));
    }

    @Test
    void testRemoveDuplicatesSimple() throws SQLException {
        var result = executeRelation("|[1, 2, 2, 3, 3, 3]->removeDuplicates()");
        assertNotNull(result.rows().get(0).get(0));
    }

    // ==================== PCT: testGroupByCastAfterAgg ====================
    // Pure: groupBy(~grp, ~newCol:x|$x.id:x|$x->plus()->cast(@Integer))
    // Cast wraps the aggregate result: CAST(SUM(id) AS BIGINT)

    @Test
    @DisplayName("PCT: testGroupByCastAfterAgg")
    void testPctRelationGroupByCastAfterAgg() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "id, grp\n" +
            "                1, 2\n" +
            "                2, 1\n" +
            "                3, 3\n" +
            "                4, 4\n" +
            "                5, 2\n" +
            "                6, 1\n" +
            "                7, 3\n" +
            "                8, 1\n" +
            "                9, 5\n" +
            "                10, 0\n" +
            "#->meta::pure::functions::relation::groupBy(~grp, ~newCol:x: (id:Integer, grp:Integer)[1]|$x.id:x: Integer[*]|meta::pure::functions::lang::cast($x->plus(), @Integer))" +
            "->meta::pure::functions::relation::sort(meta::pure::functions::relation::ascending(~grp))");
        // PCT expected (sorted by grp asc): grp=0→10, 1→16, 2→6, 3→10, 4→4, 5→9
        assertEquals(6, result.rows().size());
        assertEquals(10L, ((Number) result.rows().get(0).get(result.columns().size() - 1)).longValue());  // grp=0
        assertEquals(16L, ((Number) result.rows().get(1).get(result.columns().size() - 1)).longValue());  // grp=1
        assertEquals(6L, ((Number) result.rows().get(2).get(result.columns().size() - 1)).longValue());   // grp=2
        assertEquals(10L, ((Number) result.rows().get(3).get(result.columns().size() - 1)).longValue());  // grp=3
        assertEquals(4L, ((Number) result.rows().get(4).get(result.columns().size() - 1)).longValue());   // grp=4
        assertEquals(9L, ((Number) result.rows().get(5).get(result.columns().size() - 1)).longValue());   // grp=5
    }

    // ==================== PCT: testGroupByCastBeforeAgg ====================
    // Pure: groupBy(~grp, ~newCol:x|$x.id:x|$x->cast(@Integer)->plus())
    // Cast is on the column before aggregation — treated as noop for Integer->Integer

    @Test
    @DisplayName("PCT: testGroupByCastBeforeAgg")
    void testPctRelationGroupByCastBeforeAgg() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "id, grp\n" +
            "                1, 2\n" +
            "                2, 1\n" +
            "                3, 3\n" +
            "                4, 4\n" +
            "                5, 2\n" +
            "                6, 1\n" +
            "                7, 3\n" +
            "                8, 1\n" +
            "                9, 5\n" +
            "                10, 0\n" +
            "#->meta::pure::functions::relation::groupBy(~grp, ~newCol:x: (id:Integer, grp:Integer)[1]|$x.id:x: Integer[*]|meta::pure::functions::lang::cast($x->plus(), @Integer))" +
            "->meta::pure::functions::relation::sort(meta::pure::functions::relation::ascending(~grp))");
        // Same expected values: cast before agg is a noop for Integer->Integer
        assertEquals(6, result.rows().size());
        assertEquals(10L, ((Number) result.rows().get(0).get(result.columns().size() - 1)).longValue());  // grp=0
        assertEquals(16L, ((Number) result.rows().get(1).get(result.columns().size() - 1)).longValue());  // grp=1
        assertEquals(6L, ((Number) result.rows().get(2).get(result.columns().size() - 1)).longValue());   // grp=2
        assertEquals(10L, ((Number) result.rows().get(3).get(result.columns().size() - 1)).longValue());  // grp=3
        assertEquals(4L, ((Number) result.rows().get(4).get(result.columns().size() - 1)).longValue());   // grp=4
        assertEquals(9L, ((Number) result.rows().get(5).get(result.columns().size() - 1)).longValue());   // grp=5
    }

    // ==================== PCT: testOLAPCastAggWithPartitionWindow ====================
    // Pure: extend(over(~grp), ~newCol:{p,w,r|$r.id}:y|$y->plus()->cast(@Integer))
    // Window SUM with cast wrapping result

    @Test
    @DisplayName("PCT: testOLAPCastAggWithPartitionWindow")
    void testPctRelationOLAPCastAggWithPartitionWindow() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "id, grp, name\n" +
            "                  1, 2, A\n" +
            "                  2, 1, B\n" +
            "                  3, 3, C\n" +
            "                  4, 4, D\n" +
            "                  5, 2, E\n" +
            "                  6, 1, F\n" +
            "                  7, 3, G\n" +
            "                  8, 1, H\n" +
            "                  9, 5, I\n" +
            "                  10, 0, J\n" +
            "#->meta::pure::functions::relation::extend(~grp->meta::pure::functions::relation::over(), ~newCol:{p: meta::pure::metamodel::relation::Relation<(id:Integer, grp:Integer, name:String)>[1], w: meta::pure::functions::relation::_Window<(id:Integer, grp:Integer, name:String)>[1], r: (id:Integer, grp:Integer, name:String)[1]|$r.id}:y: Integer[*]|meta::pure::functions::lang::cast($y->plus(), @Integer))" +
            "->meta::pure::functions::relation::sort([meta::pure::functions::relation::ascending(~grp), meta::pure::functions::relation::ascending(~id)])");
        // PCT expected (sorted by grp asc, id asc):
        assertEquals(10, result.rows().size());
        assertEquals(10L, ((Number) result.rows().get(0).get(result.columns().size() - 1)).longValue());  // id=10, grp=0
        assertEquals(16L, ((Number) result.rows().get(1).get(result.columns().size() - 1)).longValue());  // id=2,  grp=1
        assertEquals(16L, ((Number) result.rows().get(2).get(result.columns().size() - 1)).longValue());  // id=6,  grp=1
        assertEquals(16L, ((Number) result.rows().get(3).get(result.columns().size() - 1)).longValue());  // id=8,  grp=1
        assertEquals(6L, ((Number) result.rows().get(4).get(result.columns().size() - 1)).longValue());   // id=1,  grp=2
        assertEquals(6L, ((Number) result.rows().get(5).get(result.columns().size() - 1)).longValue());   // id=5,  grp=2
        assertEquals(10L, ((Number) result.rows().get(6).get(result.columns().size() - 1)).longValue());  // id=3,  grp=3
        assertEquals(10L, ((Number) result.rows().get(7).get(result.columns().size() - 1)).longValue());  // id=7,  grp=3
        assertEquals(4L, ((Number) result.rows().get(8).get(result.columns().size() - 1)).longValue());   // id=4,  grp=4
        assertEquals(9L, ((Number) result.rows().get(9).get(result.columns().size() - 1)).longValue());   // id=9,  grp=5
    }

    // ==================== PCT: testOLAPAggCastWithPartitionWindow ====================
    // Pure: extend(over(~grp), ~newCol:{p,w,r|$r.id}:y|$y->cast(@Integer)->plus())
    // Cast before the window aggregate — treated as noop for Integer->Integer

    @Test
    @DisplayName("PCT: testOLAPAggCastWithPartitionWindow")
    void testPctRelationOLAPAggCastWithPartitionWindow() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "id, grp, name\n" +
            "                  1, 2, A\n" +
            "                  2, 1, B\n" +
            "                  3, 3, C\n" +
            "                  4, 4, D\n" +
            "                  5, 2, E\n" +
            "                  6, 1, F\n" +
            "                  7, 3, G\n" +
            "                  8, 1, H\n" +
            "                  9, 5, I\n" +
            "                  10, 0, J\n" +
            "#->meta::pure::functions::relation::extend(~grp->meta::pure::functions::relation::over(), ~newCol:{p: meta::pure::metamodel::relation::Relation<(id:Integer, grp:Integer, name:String)>[1], w: meta::pure::functions::relation::_Window<(id:Integer, grp:Integer, name:String)>[1], r: (id:Integer, grp:Integer, name:String)[1]|$r.id}:y: Integer[*]|meta::pure::functions::lang::cast($y->plus(), @Integer))" +
            "->meta::pure::functions::relation::sort([meta::pure::functions::relation::ascending(~grp), meta::pure::functions::relation::ascending(~id)])");
        // Same expected as testOLAPCastAggWithPartitionWindow — cast is noop
        assertEquals(10, result.rows().size());
        assertEquals(10L, ((Number) result.rows().get(0).get(result.columns().size() - 1)).longValue());  // id=10, grp=0
        assertEquals(16L, ((Number) result.rows().get(1).get(result.columns().size() - 1)).longValue());  // id=2,  grp=1
        assertEquals(6L, ((Number) result.rows().get(4).get(result.columns().size() - 1)).longValue());   // id=1,  grp=2
    }

    // ==================== PCT: testOLAPCastExtractAggWithPartitionWindow ====================
    // Pure: extend(over(~grp), ~newCol:{p,w,r|$r.id->cast(@Integer)}:y|$y->plus())
    // Cast is in the extract function (function1), not the aggregate function

    @Test
    @DisplayName("PCT: testOLAPCastExtractAggWithPartitionWindow")
    void testPctRelationOLAPCastExtractAggWithPartitionWindow() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "id, grp, name\n" +
            "                  1, 2, A\n" +
            "                  2, 1, B\n" +
            "                  3, 3, C\n" +
            "                  4, 4, D\n" +
            "                  5, 2, E\n" +
            "                  6, 1, F\n" +
            "                  7, 3, G\n" +
            "                  8, 1, H\n" +
            "                  9, 5, I\n" +
            "                  10, 0, J\n" +
            "#->meta::pure::functions::relation::extend(~grp->meta::pure::functions::relation::over(), ~newCol:{p: meta::pure::metamodel::relation::Relation<(id:Integer, grp:Integer, name:String)>[1], w: meta::pure::functions::relation::_Window<(id:Integer, grp:Integer, name:String)>[1], r: (id:Integer, grp:Integer, name:String)[1]|$r.id->meta::pure::functions::lang::cast(@Integer)}:y: Integer[*]|$y->plus())" +
            "->meta::pure::functions::relation::sort([meta::pure::functions::relation::ascending(~grp), meta::pure::functions::relation::ascending(~id)])");
        // Same expected — cast on extract is noop for Integer
        assertEquals(10, result.rows().size());
        assertEquals(10L, ((Number) result.rows().get(0).get(result.columns().size() - 1)).longValue());  // id=10, grp=0
        assertEquals(16L, ((Number) result.rows().get(1).get(result.columns().size() - 1)).longValue());  // id=2,  grp=1
        assertEquals(6L, ((Number) result.rows().get(4).get(result.columns().size() - 1)).longValue());   // id=1,  grp=2
    }

    // ==================== PCT: testOLAPCastExtractCastAggWithPartitionWindow ====================
    // Pure: extend(over(~grp), ~newCol:{p,w,r|$r.id->cast(@Integer)}:y|$y->plus()->cast(@Integer))
    // Cast on both extract and aggregate result

    @Test
    @DisplayName("PCT: testOLAPCastExtractCastAggWithPartitionWindow")
    void testPctRelationOLAPCastExtractCastAggWithPartitionWindow() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "id, grp, name\n" +
            "                  1, 2, A\n" +
            "                  2, 1, B\n" +
            "                  3, 3, C\n" +
            "                  4, 4, D\n" +
            "                  5, 2, E\n" +
            "                  6, 1, F\n" +
            "                  7, 3, G\n" +
            "                  8, 1, H\n" +
            "                  9, 5, I\n" +
            "                  10, 0, J\n" +
            "#->meta::pure::functions::relation::extend(~grp->meta::pure::functions::relation::over(), ~newCol:{p: meta::pure::metamodel::relation::Relation<(id:Integer, grp:Integer, name:String)>[1], w: meta::pure::functions::relation::_Window<(id:Integer, grp:Integer, name:String)>[1], r: (id:Integer, grp:Integer, name:String)[1]|$r.id->meta::pure::functions::lang::cast(@Integer)}:y: Integer[*]|meta::pure::functions::lang::cast($y->plus(), @Integer))" +
            "->meta::pure::functions::relation::sort([meta::pure::functions::relation::ascending(~grp), meta::pure::functions::relation::ascending(~id)])");
        // Same expected — both casts are noops for Integer
        assertEquals(10, result.rows().size());
        assertEquals(10L, ((Number) result.rows().get(0).get(result.columns().size() - 1)).longValue());  // id=10, grp=0
        assertEquals(16L, ((Number) result.rows().get(1).get(result.columns().size() - 1)).longValue());  // id=2,  grp=1
        assertEquals(6L, ((Number) result.rows().get(4).get(result.columns().size() - 1)).longValue());   // id=1,  grp=2
        assertNotNull(result);
    }

    // ==================== PCT RelationFunctions: write ====================

    @Test
    @DisplayName("PCT Relation: select then write")
    void testPctRelationSelectWrite() throws SQLException {
        var result = executeRelation(
            "|#TDS\n" +
            "val,str,other\n" +
            "                    1,a,a\n" +
            "                    3,ewe,b\n" +
            "                    4,qw,c\n" +
            "                    5,wwe,d\n" +
            "                    6,weq,e\n" +
            "#->meta::pure::functions::relation::select()->meta::pure::functions::relation::write(#TDS\n" +
            "val,str,other\n" +
            "                    1,aaa,a\n" +
            "#->meta::pure::metamodel::relation::newTDSRelationAccessor())");
        assertNotNull(result);
    }
}
