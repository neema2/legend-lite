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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@code fold()} — exercises all four FoldSpec strategies
 * and edge cases through the full compile → plan → execute pipeline.
 *
 * <p>Four strategies tested:
 * <ol>
 *   <li><strong>SameType</strong>   — T == V → {@code list_reduce(source, lambda, init)}</li>
 *   <li><strong>MapReduce</strong>  — T ≠ V, decomposable → {@code list_reduce(list_transform(source, mapper), reducer, init)}</li>
 *   <li><strong>Concatenation</strong> — body is {@code add(acc, elem)} → {@code list_concat(init, source)}</li>
 *   <li><strong>CollectionBuild</strong> — T ≠ V, non-decomposable → variant-based fallback</li>
 * </ol>
 */
public class FoldCheckerTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() { return "DuckDB"; }

    @Override
    protected SQLDialect getDialect() { return DuckDBDialect.INSTANCE; }

    @Override
    protected String getJdbcUrl() { return "jdbc:duckdb:"; }

    private static final String FOLD = "meta::pure::functions::collection::fold";
    private static final String ADD  = "meta::pure::functions::collection::add";
    private static final String CAST = "meta::pure::functions::lang::cast";
    private static final String LEN  = "meta::pure::functions::string::length";
    private static final String TO_VARIANT = "meta::pure::functions::variant::convert::toVariant";
    private static final String TO_MANY = "meta::pure::functions::variant::convert::toMany";
    private static final String TO_TYPE = "meta::pure::functions::variant::convert::to";
    private static final String TO_ONE = "meta::pure::functions::multiplicity::toOne";
    private static final String AT = "meta::pure::functions::collection::at";

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

    // ======================== Helpers ========================

    private ExecutionResult exec(String pureExpr) throws SQLException {
        return queryService.execute(
                getCompletePureModelWithRuntime(),
                "|" + pureExpr,
                "test::TestRuntime", connection);
    }

    private Object scalar(ExecutionResult r) {
        assertFalse(r.rows().isEmpty(), "Result should have at least one row");
        return r.rows().get(0).get(0);
    }

    private long scalarLong(ExecutionResult r) {
        Object v = scalar(r);
        assertInstanceOf(Number.class, v, "Expected Number but got " + (v == null ? "null" : v.getClass()));
        return ((Number) v).longValue();
    }

    private double scalarDouble(ExecutionResult r) {
        Object v = scalar(r);
        assertInstanceOf(Number.class, v, "Expected Number but got " + (v == null ? "null" : v.getClass()));
        return ((Number) v).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private List<Object> scalarList(ExecutionResult r) {
        Object v = scalar(r);
        assertInstanceOf(List.class, v, "Expected List but got " + (v == null ? "null" : v.getClass()));
        return (List<Object>) v;
    }

    // ==================== SameType: T == V ====================

    @Nested
    @DisplayName("fold() SameType — T == V → list_reduce")
    class SameType {

        @Test
        @DisplayName("integer sum: [1,2,3,4] fold(+, 0) = 10")
        void testIntegerSumZeroInit() throws SQLException {
            var r = exec("[1, 2, 3, 4]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 0)");
            assertEquals(10L, scalarLong(r));
        }

        @Test
        @DisplayName("integer sum with nonzero init: [1,2,3,4] fold(+, 7) = 17")
        void testIntegerSumWithInit() throws SQLException {
            var r = exec("[1, 2, 3, 4]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 7)");
            assertEquals(17L, scalarLong(r));
        }

        @Test
        @DisplayName("integer product: [2,3,4] fold(*, 1) = 24")
        void testIntegerProduct() throws SQLException {
            var r = exec("[2, 3, 4]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x * $y}, 1)");
            assertEquals(24L, scalarLong(r));
        }

        @Test
        @DisplayName("string concatenation: ['a','b','c','d'] fold(+, '') = 'abcd'")
        void testStringConcat() throws SQLException {
            var r = exec("['a', 'b', 'c', 'd']->" + FOLD + "({x: String[1], y: String[1]|$y + $x}, '')");
            assertEquals("abcd", scalar(r));
        }

        @Test
        @DisplayName("string concat with prefix: ['a','b','c','d'] fold(+, 'z') = 'zabcd'")
        void testStringConcatWithPrefix() throws SQLException {
            var r = exec("['a', 'b', 'c', 'd']->" + FOLD + "({x: String[1], y: String[1]|$y + $x}, 'z')");
            assertEquals("zabcd", scalar(r));
        }

        @Test
        @DisplayName("integer fold with extra constant: [1,2,3,4] fold({x,y|x+y+2}, 7) = 25")
        void testIntegerWithExtraArithmetic() throws SQLException {
            // 7 → 7+1+2=10 → 10+2+2=14 → 14+3+2=19 → 19+4+2=25
            var r = exec("[1, 2, 3, 4]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y + 2}, 7)");
            assertEquals(25L, scalarLong(r));
        }

        @Test
        @DisplayName("float sum: [1.5, 2.5, 3.0] fold(+, 0.0) = 7.0")
        void testFloatSum() throws SQLException {
            var r = exec("[1.5, 2.5, 3.0]->" + FOLD + "({x: Float[1], y: Float[1]|$x + $y}, 0.0)");
            assertEquals(7.0, scalarDouble(r), 0.001);
        }

        @Test
        @DisplayName("integer subtraction chain: [10, 3, 2] fold(-, 100) = 85")
        void testSubtractionFold() throws SQLException {
            // 100 → 100-10=90 → 90-3=87 → 87-2=85
            var r = exec("[10, 3, 2]->" + FOLD + "({x: Integer[1], y: Integer[1]|$y - $x}, 100)");
            assertEquals(85L, scalarLong(r));
        }

        @Test
        @DisplayName("single element: [42] fold(+, 0) = 42")
        void testSingleElement() throws SQLException {
            var r = exec("[42]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 0)");
            assertEquals(42L, scalarLong(r));
        }

        @Test
        @DisplayName("single element returns init when identity: [5] fold(+, 10) = 15")
        void testSingleElementWithInit() throws SQLException {
            var r = exec("[5]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 10)");
            assertEquals(15L, scalarLong(r));
        }
    }

    // ==================== MapReduce: T ≠ V, decomposable ====================

    @Nested
    @DisplayName("fold() MapReduce — T ≠ V, body decomposable into map+reduce")
    class MapReduce {

        @Test
        @DisplayName("string→int: ['one','two'] fold({val,acc|acc+length(val)}, 1) = 7")
        void testStringLengthSum() throws SQLException {
            var r = exec("['one', 'two']->" + FOLD +
                    "({val: String[1], acc: Integer[1]|$acc + $val->" + LEN + "()}, 1)");
            assertEquals(7L, scalarLong(r));
        }

        @Test
        @DisplayName("string→int with zero init: ['hello','world'] fold({v,a|a+length(v)}, 0) = 10")
        void testStringLengthSumZeroInit() throws SQLException {
            var r = exec("['hello', 'world']->" + FOLD +
                    "({v: String[1], a: Integer[1]|$a + $v->" + LEN + "()}, 0)");
            assertEquals(10L, scalarLong(r));
        }

        @Test
        @DisplayName("variant→int: toVariant→toMany→fold(acc+to(@Int)→toOne(), 1) = 7")
        void testFoldFromVariant() throws SQLException {
            var r = exec("[1, 2, 3]->" + TO_VARIANT + "()->" + TO_MANY +
                    "(@Integer)->" + FOLD +
                    "({val: Integer[1], acc: Integer[1]|$acc + $val}, 1)");
            assertEquals(7L, scalarLong(r));
        }

        @Test
        @DisplayName("variant elements with cast: toVariant→toMany(@Variant)→fold(acc + to(@Int)→toOne(), 1) = 7")
        void testFoldFromVariantAsPrimitive() throws SQLException {
            var r = exec("[1, 2, 3]->" + TO_VARIANT + "()->" + TO_MANY +
                    "(@meta::pure::metamodel::variant::Variant)->" + FOLD +
                    "({val: meta::pure::metamodel::variant::Variant[1], acc: Integer[1]|$acc + $val->" +
                    TO_TYPE + "(@Integer)->" + TO_ONE + "()}, 1)");
            assertEquals(7L, scalarLong(r));
        }

        @Test
        @DisplayName("struct→string: fold P_Person structs into name list string")
        void testFoldStructToString() throws SQLException {
            var r = exec("[^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Pierre',lastName='Doe'), " +
                    "^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Kevin',lastName='RoeDoe'), " +
                    "^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Andrew',lastName='Some_LName')]" +
                    "->" + FOLD + "({p: meta::pure::functions::string::tests::plus::model::P_Person[1], s: String[1]|$s + '; ' + $p.lastName->" +
                    AT + "(0) + ', ' + $p.firstName->" + AT + "(0)}, 'names')");
            assertEquals("names; Doe, Pierre; RoeDoe, Kevin; Some_LName, Andrew", scalar(r));
        }
    }

    // ==================== Concatenation: body is add(acc, elem) ====================

    @Nested
    @DisplayName("fold() Concatenation — body is add(acc, elem) → list_concat")
    class Concatenation {

        @Test
        @DisplayName("basic add fold: [1,2,3,4] fold(add, [-1,0]) = [-1,0,1,2,3,4]")
        void testBasicConcatenation() throws SQLException {
            var r = exec("[1, 2, 3, 4]->" + FOLD +
                    "({x: Integer[1], y: Integer[2]|$y->" + ADD + "($x)}, [-1, 0])");
            var arr = scalarList(r);
            assertEquals(6, arr.size());
            assertEquals(-1, ((Number) arr.get(0)).intValue());
            assertEquals(0, ((Number) arr.get(1)).intValue());
            assertEquals(1, ((Number) arr.get(2)).intValue());
            assertEquals(2, ((Number) arr.get(3)).intValue());
            assertEquals(3, ((Number) arr.get(4)).intValue());
            assertEquals(4, ((Number) arr.get(5)).intValue());
        }

        @Test
        @DisplayName("empty accumulator: [1,2,3] fold(add, []) = [1,2,3]")
        void testEmptyAccumulator() throws SQLException {
            var r = exec("[1, 2, 3]->" + FOLD +
                    "({val: Integer[1], acc: meta::pure::metamodel::type::Nil[0]|$acc->" + ADD + "($val)}, [])");
            var arr = scalarList(r);
            assertEquals(3, arr.size());
            assertEquals(1, ((Number) arr.get(0)).intValue());
            assertEquals(2, ((Number) arr.get(1)).intValue());
            assertEquals(3, ((Number) arr.get(2)).intValue());
        }

        @Test
        @DisplayName("single value source: 1 fold(add, []) = [1]")
        void testSingleValueSource() throws SQLException {
            var r = exec("1->" + FOLD +
                    "({val: Integer[1], acc: meta::pure::metamodel::type::Nil[0]|$acc->" + ADD + "($val)}, [])");
            var arr = scalarList(r);
            assertEquals(1, arr.size());
            assertEquals(1, ((Number) arr.get(0)).intValue());
        }

        @Test
        @DisplayName("empty list + empty identity: []-cast(@Int) fold(add, []-cast(@Any)) = []")
        void testEmptyListAndEmptyIdentity() throws SQLException {
            var r = exec("[]->" + CAST + "(@Integer)->" + FOLD +
                    "({val: Integer[1], acc: meta::pure::metamodel::type::Any[0]|$acc->" + ADD +
                    "($val)}, []->" + CAST + "(@meta::pure::metamodel::type::Any))");
            var arr = scalarList(r);
            assertEquals(0, arr.size());
        }
    }

    // ==================== CollectionBuild: T ≠ V, non-decomposable ====================

    @Nested
    @DisplayName("fold() CollectionBuild — complex non-decomposable lambda")
    class CollectionBuild {

        @Test
        @DisplayName("if+size+tail sliding window: [1,2,3,4] fold(if(size<3, add, add→tail), [-1,0]) = [2,3,4]")
        void testSlidingWindow() throws SQLException {
            var r = exec("[1, 2, 3, 4]->" + FOLD +
                    "({x: Integer[1], y: Integer[1..3]|meta::pure::functions::lang::if(" +
                    "$y->meta::pure::functions::collection::size() < 3, " +
                    "|$y->" + ADD + "($x), " +
                    "|$y->" + ADD + "($x)->meta::pure::functions::collection::tail())}, [-1, 0])");
            var arr = scalarList(r);
            assertEquals(3, arr.size());
            assertEquals(2, ((Number) arr.get(0)).intValue());
            assertEquals(3, ((Number) arr.get(1)).intValue());
            assertEquals(4, ((Number) arr.get(2)).intValue());
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("fold() edge cases")
    class EdgeCases {

        @Test
        @DisplayName("large list: fold sum of 1..100 = 5050")
        void testLargeListSum() throws SQLException {
            // Build [1, 2, ..., 100]
            var sb = new StringBuilder("[");
            for (int i = 1; i <= 100; i++) {
                if (i > 1) sb.append(", ");
                sb.append(i);
            }
            sb.append("]");
            var r = exec(sb + "->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 0)");
            assertEquals(5050L, scalarLong(r));
        }

        @Test
        @DisplayName("negative numbers: [-5, -3, 10] fold(+, 0) = 2")
        void testNegativeNumbers() throws SQLException {
            var r = exec("[-5, -3, 10]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 0)");
            assertEquals(2L, scalarLong(r));
        }

        @Test
        @DisplayName("all zeros: [0, 0, 0] fold(+, 0) = 0")
        void testAllZeros() throws SQLException {
            var r = exec("[0, 0, 0]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x + $y}, 0)");
            assertEquals(0L, scalarLong(r));
        }

        @Test
        @DisplayName("string fold to count words: ['the','quick','brown'] count via length sum = 13")
        void testWordLengthCount() throws SQLException {
            var r = exec("['the', 'quick', 'brown']->" + FOLD +
                    "({w: String[1], acc: Integer[1]|$acc + $w->" + LEN + "()}, 0)");
            assertEquals(13L, scalarLong(r));
        }

        @Test
        @DisplayName("lambda param names: custom naming works (elem/accumulator)")
        void testCustomParamNames() throws SQLException {
            var r = exec("[10, 20, 30]->" + FOLD +
                    "({elem: Integer[1], accumulator: Integer[1]|$elem + $accumulator}, 0)");
            assertEquals(60L, scalarLong(r));
        }

        @Test
        @DisplayName("fold with multiplication and large init: [2,3] fold(*, 100) = 600")
        void testMultiplyWithLargeInit() throws SQLException {
            var r = exec("[2, 3]->" + FOLD + "({x: Integer[1], y: Integer[1]|$x * $y}, 100)");
            assertEquals(600L, scalarLong(r));
        }
    }

    // ==================== Chained Operations ====================

    @Nested
    @DisplayName("fold() chained with other operations")
    class Chained {

        @Test
        @DisplayName("fold result in toString: fold(+, 0) → toString()")
        void testFoldThenToString() throws SQLException {
            var r = exec("[1, 2, 3]->" + FOLD +
                    "({x: Integer[1], y: Integer[1]|$x + $y}, 0)" +
                    "->meta::pure::functions::string::toString()");
            assertEquals("6", scalar(r));
        }

        @Test
        @DisplayName("fold result in comparison: fold(+, 0) > 5 → true")
        void testFoldThenCompare() throws SQLException {
            var r = exec("[1, 2, 3]->" + FOLD +
                    "({x: Integer[1], y: Integer[1]|$x + $y}, 0) > 5");
            assertEquals(true, scalar(r));
        }

        @Test
        @DisplayName("fold result in arithmetic: fold(+, 0) * 2 = 12")
        void testFoldThenArithmetic() throws SQLException {
            var r = exec("[1, 2, 3]->" + FOLD +
                    "({x: Integer[1], y: Integer[1]|$x + $y}, 0) * 2");
            assertEquals(12L, scalarLong(r));
        }
    }
}
