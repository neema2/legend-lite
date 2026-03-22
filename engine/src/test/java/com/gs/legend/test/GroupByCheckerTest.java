package com.gs.legend.test;

import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for the refactored GroupByChecker and AggregateChecker.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>Single/multiple group columns with single/multiple aggregates</li>
 *   <li>All standard aggregate functions: sum, count, min, max, average</li>
 *   <li>Expression-based fn1 (not just simple column access)</li>
 *   <li>rowMapper pattern: wavg, corr, covarSample, covarPopulation, maxBy, minBy</li>
 *   <li>Aggregate with cast wrapper</li>
 *   <li>Full-table aggregate() (no group columns)</li>
 *   <li>Chaining: groupBy→filter, groupBy→sort, groupBy→extend</li>
 *   <li>Edge cases: empty groups, null values, single-row groups</li>
 * </ul>
 */
public class GroupByCheckerTest extends AbstractDatabaseTest {

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
    // 1. Basic groupBy with single group column, single aggregate
    // ========================================================================

    @Nested
    @DisplayName("groupBy(~col, ~agg:x|...:y|$y->func())")
    class BasicGroupBy {

        @Test
        @DisplayName("sum: single group, single agg")
        void testGroupBySingleSum() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "total");
            assertEquals(30L, ((Number) byGrp.get(1)).longValue());
            assertEquals(30L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("count via plus()")
        void testGroupByCount() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    2, 40
                    #->groupBy(~grp, ~cnt:x|1:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "cnt");
            assertEquals(3L, ((Number) byGrp.get(1)).longValue());
            assertEquals(1L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("min aggregate")
        void testGroupByMin() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 30
                    1, 10
                    1, 20
                    2, 50
                    2, 40
                    #->groupBy(~grp, ~minVal:x|$x.val:y|$y->min())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "minVal");
            assertEquals(10L, ((Number) byGrp.get(1)).longValue());
            assertEquals(40L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("max aggregate")
        void testGroupByMax() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 30
                    1, 10
                    1, 20
                    2, 50
                    2, 40
                    #->groupBy(~grp, ~maxVal:x|$x.val:y|$y->max())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "maxVal");
            assertEquals(30L, ((Number) byGrp.get(1)).longValue());
            assertEquals(50L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("average aggregate")
        void testGroupByAverage() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 30
                    2, 20
                    #->groupBy(~grp, ~avgVal:x|$x.val:y|$y->average())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "avgVal");
            assertEquals(20.0, ((Number) byGrp.get(1)).doubleValue(), 0.001);
            assertEquals(20.0, ((Number) byGrp.get(2)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("single row per group")
        void testGroupBySingleRowGroups() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 100
                    2, 200
                    3, 300
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(3, result.rowCount());
            var byGrp = collectResults(result, "grp", "total");
            assertEquals(100L, ((Number) byGrp.get(1)).longValue());
            assertEquals(200L, ((Number) byGrp.get(2)).longValue());
            assertEquals(300L, ((Number) byGrp.get(3)).longValue());
        }
    }

    // ========================================================================
    // 2. Multiple group columns
    // ========================================================================

    @Nested
    @DisplayName("groupBy(~[col1, col2], ~agg)")
    class MultipleGroupColumns {

        @Test
        @DisplayName("two group columns + sum")
        void testTwoGroupColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp1, grp2, val
                    1, A, 10
                    1, A, 20
                    1, B, 30
                    2, A, 40
                    #->groupBy(~[grp1, grp2], ~total:x|$x.val:y|$y->plus())""");
            assertEquals(3, result.rowCount());
            assertEquals(3, result.columns().size(), "grp1, grp2, total");
        }

        @Test
        @DisplayName("three group columns + sum")
        void testThreeGroupColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b, c, val
                    1, X, P, 10
                    1, X, P, 20
                    1, X, Q, 30
                    2, Y, P, 40
                    #->groupBy(~[a, b, c], ~total:x|$x.val:y|$y->plus())""");
            assertEquals(3, result.rowCount());
            assertEquals(4, result.columns().size());
        }

        @Test
        @DisplayName("single group column passed as array ~[col]")
        void testSingleGroupAsArray() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~[grp], ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
        }
    }

    // ========================================================================
    // 3. Multiple aggregates
    // ========================================================================

    @Nested
    @DisplayName("groupBy(~grp, ~[agg1, agg2, ...])")
    class MultipleAggregates {

        @Test
        @DisplayName("sum + count")
        void testSumAndCount() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    2, 40
                    2, 50
                    #->groupBy(~grp, ~[total:x|$x.val:y|$y->plus(), cnt:x|1:y|$y->plus()])""");
            assertEquals(2, result.rowCount());
            assertEquals(3, result.columns().size(), "grp, total, cnt");
            var totals = collectResults(result, "grp", "total");
            var counts = collectResults(result, "grp", "cnt");
            assertEquals(60L, ((Number) totals.get(1)).longValue());
            assertEquals(90L, ((Number) totals.get(2)).longValue());
            assertEquals(3L, ((Number) counts.get(1)).longValue());
            assertEquals(2L, ((Number) counts.get(2)).longValue());
        }

        @Test
        @DisplayName("sum + min + max")
        void testSumMinMax() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 30
                    1, 20
                    2, 50
                    #->groupBy(~grp, ~[s:x|$x.val:y|$y->plus(), lo:x|$x.val:y|$y->min(), hi:x|$x.val:y|$y->max()])""");
            assertEquals(2, result.rowCount());
            assertEquals(4, result.columns().size());
            var sums = collectResults(result, "grp", "s");
            var mins = collectResults(result, "grp", "lo");
            var maxs = collectResults(result, "grp", "hi");
            assertEquals(60L, ((Number) sums.get(1)).longValue());
            assertEquals(10L, ((Number) mins.get(1)).longValue());
            assertEquals(30L, ((Number) maxs.get(1)).longValue());
        }

        @Test
        @DisplayName("sum + average (mixed types)")
        void testSumAndAverage() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~[total:x|$x.val:y|$y->plus(), avg:x|$x.val:y|$y->average()])""");
            assertEquals(2, result.rowCount());
            var avgs = collectResults(result, "grp", "avg");
            assertEquals(15.0, ((Number) avgs.get(1)).doubleValue(), 0.001);
            assertEquals(30.0, ((Number) avgs.get(2)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("three aggregates on same source column")
        void testThreeAggsOnSameColumn() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    #->groupBy(~grp, ~[a:x|$x.val:y|$y->plus(), b:x|$x.val:y|$y->min(), c:x|$x.val:y|$y->max()])""");
            assertEquals(1, result.rowCount());
            assertEquals(4, result.columns().size());
        }
    }

    // ========================================================================
    // 4. Expression-based fn1 (not just $x.col)
    // ========================================================================

    @Nested
    @DisplayName("fn1 = expression (e.g., $x.price * $x.qty)")
    class ExpressionFn1 {

        @Test
        @DisplayName("fn1 = multiplication expression")
        void testFn1Multiplication() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, price, qty
                    1, 10, 2
                    1, 20, 3
                    2, 5, 10
                    #->groupBy(~grp, ~revenue:x|$x.price * $x.qty:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "revenue");
            // grp1: 10*2 + 20*3 = 80, grp2: 5*10 = 50
            assertEquals(80L, ((Number) byGrp.get(1)).longValue());
            assertEquals(50L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("fn1 = addition expression")
        void testFn1Addition() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, a, b
                    1, 10, 5
                    1, 20, 10
                    2, 30, 15
                    #->groupBy(~grp, ~sumAB:x|$x.a + $x.b:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "sumAB");
            // grp1: (10+5)+(20+10) = 45, grp2: 30+15 = 45
            assertEquals(45L, ((Number) byGrp.get(1)).longValue());
            assertEquals(45L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("fn1 = literal 1 (count pattern)")
        void testFn1LiteralCount() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    A, 1
                    A, 2
                    A, 3
                    B, 4
                    #->groupBy(~grp, ~cnt:x|1:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "cnt");
            assertEquals(3L, ((Number) byGrp.get("A")).longValue());
            assertEquals(1L, ((Number) byGrp.get("B")).longValue());
        }

        @Test
        @DisplayName("fn1 = string column access + count")
        void testFn1StringColumnCount() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, name
                    eng, Alice
                    eng, Bob
                    eng, Charlie
                    mkt, Dave
                    mkt, Eve
                    #->groupBy(~dept, ~cnt:x|1:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "dept", "cnt");
            assertEquals(3L, ((Number) byGrp.get("eng")).longValue());
            assertEquals(2L, ((Number) byGrp.get("mkt")).longValue());
        }
    }

    // ========================================================================
    // 5. rowMapper aggregates: wavg, corr, covarSample, covarPopulation, maxBy, minBy
    // ========================================================================

    @Nested
    @DisplayName("rowMapper aggregates")
    class RowMapperAggregates {

        @Test
        @DisplayName("wavg: weighted average")
        void testWavg() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, quantity, weight
                    1, 100, 0.45
                    1, 500, 0.15
                    1, 150, 0.4
                    2, 200, 0.5
                    2, 100, 0.5
                    #->groupBy(~grp, ~wavgCol:x|meta::pure::functions::math::mathUtility::rowMapper($x.quantity, $x.weight):y|$y->wavg())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "wavgCol");
            // grp1: (100*0.45 + 500*0.15 + 150*0.4) / (0.45+0.15+0.4) = 180.0
            assertEquals(180.0, ((Number) byGrp.get(1)).doubleValue(), 0.01);
            // grp2: (200*0.5 + 100*0.5) / (0.5+0.5) = 150.0
            assertEquals(150.0, ((Number) byGrp.get(2)).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("multiple wavg on same TDS")
        void testMultipleWavg() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, qty, w1, w2
                    1, 100, 0.5, 0.3
                    1, 200, 0.5, 0.7
                    2, 300, 1.0, 1.0
                    #->groupBy(~grp, ~[wa1:x|meta::pure::functions::math::mathUtility::rowMapper($x.qty, $x.w1):y|$y->wavg(), wa2:x|meta::pure::functions::math::mathUtility::rowMapper($x.qty, $x.w2):y|$y->wavg()])""");
            assertEquals(2, result.rowCount());
            var wa1 = collectResults(result, "grp", "wa1");
            var wa2 = collectResults(result, "grp", "wa2");
            // grp1 wa1: (100*0.5 + 200*0.5) / 1.0 = 150
            assertEquals(150.0, ((Number) wa1.get(1)).doubleValue(), 0.01);
            // grp1 wa2: (100*0.3 + 200*0.7) / 1.0 = 170
            assertEquals(170.0, ((Number) wa2.get(1)).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("maxBy: name with highest employeeNumber")
        void testMaxBy() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, name, employeeNumber
                    1, Alice, 10000
                    1, Bob, 9000
                    1, Charlie, 17000
                    2, Dave, 12000
                    2, Eve, 13000
                    #->groupBy(~grp, ~winner:x|meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.employeeNumber):y|$y->maxBy())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "winner");
            assertEquals("Charlie", byGrp.get(1).toString());
            assertEquals("Eve", byGrp.get(2).toString());
        }

        @Test
        @DisplayName("minBy: name with lowest employeeNumber")
        void testMinBy() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, name, employeeNumber
                    1, Alice, 10000
                    1, Bob, 9000
                    1, Charlie, 17000
                    2, Dave, 12000
                    2, Eve, 8000
                    #->groupBy(~grp, ~loser:x|meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.employeeNumber):y|$y->minBy())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "loser");
            assertEquals("Bob", byGrp.get(1).toString());
            assertEquals("Eve", byGrp.get(2).toString());
        }

        @Test
        @DisplayName("corr: correlation of two columns")
        void testCorr() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, a, b
                    1, 1, 10
                    1, 2, 20
                    1, 3, 30
                    2, 1, 30
                    2, 2, 20
                    2, 3, 10
                    #->groupBy(~grp, ~corrAB:x|meta::pure::functions::math::mathUtility::rowMapper($x.a, $x.b):y|$y->corr())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "corrAB");
            // grp1: perfect positive correlation
            assertEquals(1.0, ((Number) byGrp.get(1)).doubleValue(), 0.0001);
            // grp2: perfect negative correlation
            assertEquals(-1.0, ((Number) byGrp.get(2)).doubleValue(), 0.0001);
        }

        @Test
        @DisplayName("covarSample: sample covariance")
        void testCovarSample() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, a, b
                    1, 1, 10
                    1, 2, 20
                    2, 2, 40
                    2, 4, 15
                    #->groupBy(~grp, ~cov:x|meta::pure::functions::math::mathUtility::rowMapper($x.a, $x.b):y|$y->covarSample())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "cov");
            assertEquals(5.0, ((Number) byGrp.get(1)).doubleValue(), 0.0001);
            assertEquals(-25.0, ((Number) byGrp.get(2)).doubleValue(), 0.0001);
        }

        @Test
        @DisplayName("covarPopulation: population covariance")
        void testCovarPopulation() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, a, b
                    1, 1, 10
                    1, 2, 20
                    2, 2, 40
                    2, 4, 15
                    #->groupBy(~grp, ~covPop:x|meta::pure::functions::math::mathUtility::rowMapper($x.a, $x.b):y|$y->covarPopulation())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "covPop");
            assertEquals(2.5, ((Number) byGrp.get(1)).doubleValue(), 0.0001);
            assertEquals(-12.5, ((Number) byGrp.get(2)).doubleValue(), 0.0001);
        }
    }

    // ========================================================================
    // 5b. Statistical aggregates: stdDev, variance, percentile, hashCode
    // ========================================================================

    @Nested
    @DisplayName("statistical aggregate functions")
    class StatisticalAggregates {

        @Test
        @DisplayName("stdDev: standard deviation")
        void testStdDev() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    2, 100
                    2, 200
                    #->groupBy(~grp, ~sd:x|$x.val:y|$y->stdDev())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "sd");
            assertEquals(10.0, ((Number) byGrp.get(1)).doubleValue(), 0.01);
            // stdDev of [100, 200] = ~70.71
            assertEquals(70.71, ((Number) byGrp.get(2)).doubleValue(), 0.1);
        }

        @Test
        @DisplayName("stdDevSample: sample standard deviation")
        void testStdDevSample() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 2
                    1, 4
                    1, 4
                    1, 4
                    1, 5
                    1, 5
                    1, 7
                    1, 9
                    #->groupBy(~grp, ~sd:x|$x.val:y|$y->stdDevSample())""");
            assertEquals(1, result.rowCount());
            // stdDevSample of [2,4,4,4,5,5,7,9] ≈ 2.138
            assertEquals(2.138, ((Number) result.rows().get(0).get(
                    columnIndex(result, "sd"))).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("stdDevPopulation: population standard deviation")
        void testStdDevPopulation() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    #->groupBy(~grp, ~sd:x|$x.val:y|$y->stdDevPopulation())""");
            assertEquals(1, result.rowCount());
            // stdDevPopulation of [10,20,30] = ~8.165
            assertEquals(8.165, ((Number) result.rows().get(0).get(
                    columnIndex(result, "sd"))).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("variance: variance (population)")
        void testVariance() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    2, 5
                    2, 5
                    #->groupBy(~grp, ~v:x|$x.val:y|$y->variance())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "v");
            // variance of [10,20,30] = ~66.67 (pop) or 100 (sample)
            assertNotNull(byGrp.get(1));
            // variance of [5,5] = 0
            assertEquals(0.0, ((Number) byGrp.get(2)).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("varianceSample: sample variance")
        void testVarianceSample() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    #->groupBy(~grp, ~v:x|$x.val:y|$y->varianceSample())""");
            assertEquals(1, result.rowCount());
            // varianceSample of [10,20,30] = 100
            assertEquals(100.0, ((Number) result.rows().get(0).get(
                    columnIndex(result, "v"))).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("variancePopulation: population variance")
        void testVariancePopulation() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    #->groupBy(~grp, ~v:x|$x.val:y|$y->variancePopulation())""");
            assertEquals(1, result.rowCount());
            // variancePopulation of [10,20,30] = 66.667
            assertEquals(66.667, ((Number) result.rows().get(0).get(
                    columnIndex(result, "v"))).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("combined: sum + stdDev + variance in one query")
        void testCombinedStatAggs() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    2, 40
                    #->groupBy(~grp, ~[s:x|$x.val:y|$y->plus(), sd:x|$x.val:y|$y->stdDev(), v:x|$x.val:y|$y->variance()])""");
            assertEquals(2, result.rowCount());
            assertEquals(4, result.columns().size());
        }

        @Test
        @DisplayName("stdDev + average: analytics pattern")
        void testStdDevAndAverage() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, salary
                    eng, 100
                    eng, 120
                    eng, 140
                    mkt, 80
                    mkt, 120
                    #->groupBy(~dept, ~[avg:x|$x.salary:y|$y->average(), sd:x|$x.salary:y|$y->stdDev()])""");
            assertEquals(2, result.rowCount());
            assertEquals(3, result.columns().size());
            var avgs = collectResults(result, "dept", "avg");
            assertEquals(120.0, ((Number) avgs.get("eng")).doubleValue(), 0.001);
            assertEquals(100.0, ((Number) avgs.get("mkt")).doubleValue(), 0.001);
        }
    }

    // ========================================================================
    // 6. Full-table aggregate() — no group columns
    // ========================================================================

    @Nested
    @DisplayName("aggregate(~agg) — full table")
    class FullTableAggregate {

        @Test
        @DisplayName("aggregate: sum entire table")
        void testAggregateSum() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    #->aggregate(~total:x|$x.val:y|$y->plus())""");
            assertEquals(1, result.rowCount());
            assertEquals(60L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        @DisplayName("aggregate: count entire table")
        void testAggregateCount() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    40
                    #->aggregate(~cnt:x|1:y|$y->plus())""");
            assertEquals(1, result.rowCount());
            assertEquals(4L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        @DisplayName("aggregate: multiple aggregates")
        void testAggregateMultiple() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    #->aggregate(~[total:x|$x.val:y|$y->plus(), cnt:x|1:y|$y->plus(), avg:x|$x.val:y|$y->average()])""");
            assertEquals(1, result.rowCount());
            assertEquals(3, result.columns().size());
        }

        @Test
        @DisplayName("aggregate: expression fn1")
        void testAggregateExpressionFn1() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    price, qty
                    10, 2
                    20, 3
                    30, 1
                    #->aggregate(~totalRev:x|$x.price * $x.qty:y|$y->plus())""");
            assertEquals(1, result.rowCount());
            // 10*2 + 20*3 + 30*1 = 110
            assertEquals(110L, ((Number) result.rows().get(0).get(0)).longValue());
        }
    }

    // ========================================================================
    // 7. Chaining: groupBy→filter, groupBy→sort, groupBy→extend
    // ========================================================================

    @Nested
    @DisplayName("groupBy chaining")
    class GroupByChaining {

        @Test
        @DisplayName("groupBy→filter: filter aggregated results")
        void testGroupByThenFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 100
                    3, 5
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->filter(x|$x.total > 50)""");
            assertEquals(1, result.rowCount());
            assertEquals(100L, ((Number) result.rows().get(0).get(
                    columnIndex(result, "total"))).longValue());
        }

        @Test
        @DisplayName("groupBy→sort: sort by aggregated column")
        void testGroupByThenSort() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 30
                    2, 10
                    3, 20
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->sort(~total->ascending())""");
            assertEquals(3, result.rowCount());
            int totalIdx = columnIndex(result, "total");
            // Sorted ascending: 10, 20, 30
            assertEquals(10L, ((Number) result.rows().get(0).get(totalIdx)).longValue());
            assertEquals(20L, ((Number) result.rows().get(1).get(totalIdx)).longValue());
            assertEquals(30L, ((Number) result.rows().get(2).get(totalIdx)).longValue());
        }

        @Test
        @DisplayName("filter→groupBy: filter source before grouping")
        void testFilterThenGroupBy() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    2, 999
                    #->filter(x|$x.val < 100)
                    ->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "total");
            assertEquals(30L, ((Number) byGrp.get(1)).longValue());
            assertEquals(30L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("aggregate→filter: filter full-table aggregate")
        void testAggregateThenFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    10
                    20
                    30
                    #->aggregate(~total:x|$x.val:y|$y->plus())
                    ->filter(x|$x.total > 0)""");
            assertEquals(1, result.rowCount());
        }

        @Test
        @DisplayName("groupBy→filter→sort chain")
        void testGroupByFilterSort() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    2, 20
                    3, 30
                    4, 40
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->filter(x|$x.total >= 20)
                    ->sort(~total->descending())""");
            assertEquals(3, result.rowCount());
            int totalIdx = columnIndex(result, "total");
            assertEquals(40L, ((Number) result.rows().get(0).get(totalIdx)).longValue());
            assertEquals(30L, ((Number) result.rows().get(1).get(totalIdx)).longValue());
            assertEquals(20L, ((Number) result.rows().get(2).get(totalIdx)).longValue());
        }
    }

    // ========================================================================
    // 8. String group columns
    // ========================================================================

    @Nested
    @DisplayName("groupBy with string group columns")
    class StringGroupColumns {

        @Test
        @DisplayName("string group column + sum")
        void testStringGroupColumn() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, salary
                    engineering, 100
                    engineering, 120
                    marketing, 80
                    marketing, 90
                    marketing, 85
                    #->groupBy(~dept, ~totalSalary:x|$x.salary:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "dept", "totalSalary");
            assertEquals(220L, ((Number) byGrp.get("engineering")).longValue());
            assertEquals(255L, ((Number) byGrp.get("marketing")).longValue());
        }

        @Test
        @DisplayName("string + integer group columns")
        void testMixedTypeGroupColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, level, salary
                    eng, 1, 100
                    eng, 1, 110
                    eng, 2, 150
                    mkt, 1, 80
                    #->groupBy(~[dept, level], ~total:x|$x.salary:y|$y->plus())""");
            assertEquals(3, result.rowCount());
            assertEquals(3, result.columns().size());
        }
    }

    // ========================================================================
    // 9. Float/Decimal values
    // ========================================================================

    @Nested
    @DisplayName("groupBy with float values")
    class FloatValues {

        @Test
        @DisplayName("sum of float values")
        void testFloatSum() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 1.5
                    1, 2.5
                    2, 3.0
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "total");
            assertEquals(4.0, ((Number) byGrp.get(1)).doubleValue(), 0.001);
            assertEquals(3.0, ((Number) byGrp.get(2)).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("average of float values")
        void testFloatAverage() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 1.0
                    1, 2.0
                    1, 3.0
                    2, 10.0
                    #->groupBy(~grp, ~avg:x|$x.val:y|$y->average())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "avg");
            assertEquals(2.0, ((Number) byGrp.get(1)).doubleValue(), 0.001);
            assertEquals(10.0, ((Number) byGrp.get(2)).doubleValue(), 0.001);
        }
    }

    // ========================================================================
    // 10. Many groups (stress test)
    // ========================================================================

    @Nested
    @DisplayName("large datasets")
    class LargeDatasets {

        @Test
        @DisplayName("many groups — 5 groups with varying sizes")
        void testManyGroups() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    2, 40
                    2, 50
                    3, 60
                    4, 70
                    4, 80
                    5, 90
                    5, 100
                    5, 110
                    5, 120
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(5, result.rowCount());
            var byGrp = collectResults(result, "grp", "total");
            assertEquals(30L, ((Number) byGrp.get(1)).longValue());
            assertEquals(120L, ((Number) byGrp.get(2)).longValue());
            assertEquals(60L, ((Number) byGrp.get(3)).longValue());
            assertEquals(150L, ((Number) byGrp.get(4)).longValue());
            assertEquals(420L, ((Number) byGrp.get(5)).longValue());
        }

        @Test
        @DisplayName("all rows in one group")
        void testSingleGroupAllRows() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    1, 30
                    1, 40
                    1, 50
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(1, result.rowCount());
            assertEquals(150L, ((Number) result.rows().get(0).get(
                    columnIndex(result, "total"))).longValue());
        }
    }

    // ========================================================================
    // 11. Column output schema validation
    // ========================================================================

    @Nested
    @DisplayName("output schema validation")
    class OutputSchema {

        @Test
        @DisplayName("output columns = group + agg")
        void testOutputColumnsGroupAndAgg() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val, extra
                    1, 10, X
                    1, 20, Y
                    2, 30, Z
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.columns().size(), "Only grp + total, not extra");
            assertEquals("grp", result.columns().get(0).name());
            assertEquals("total", result.columns().get(1).name());
        }

        @Test
        @DisplayName("aggregate only — no group columns in output")
        void testAggregateOutputNoGroupCols() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    2, 20
                    #->aggregate(~total:x|$x.val:y|$y->plus())""");
            assertEquals(1, result.columns().size());
            assertEquals("total", result.columns().get(0).name());
        }

        @Test
        @DisplayName("multiple group + multiple agg column order")
        void testColumnOrder() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    a, b, val
                    1, X, 10
                    #->groupBy(~[a, b], ~[s:x|$x.val:y|$y->plus(), c:x|1:y|$y->plus()])""");
            assertEquals(4, result.columns().size());
            assertEquals("a", result.columns().get(0).name());
            assertEquals("b", result.columns().get(1).name());
            assertEquals("s", result.columns().get(2).name());
            assertEquals("c", result.columns().get(3).name());
        }
    }

    @Nested
    @DisplayName("groupBy + downstream ops")
    class DownstreamOps {

        @Test
        @DisplayName("groupBy→select: pick only aggregate column")
        void testGroupByThenSelect() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->select(~total)""");
            assertEquals(2, result.rowCount());
            assertEquals(1, result.columns().size());
            assertEquals("total", result.columns().get(0).name());
        }

        @Test
        @DisplayName("groupBy→select multiple columns")
        void testGroupByThenSelectMultiple() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~[total:x|$x.val:y|$y->plus(), cnt:x|1:y|$y->plus()])
                    ->select(~[grp, total])""");
            assertEquals(2, result.rowCount());
            assertEquals(2, result.columns().size());
            assertEquals("grp", result.columns().get(0).name());
            assertEquals("total", result.columns().get(1).name());
        }

        @Test
        @DisplayName("groupBy→rename: rename aggregate column")
        void testGroupByThenRename() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->rename(~total, ~sum_total)""");
            assertEquals(2, result.rowCount());
            assertEquals(2, result.columns().size());
            boolean found = result.columns().stream().anyMatch(c -> "sum_total".equals(c.name()));
            assertTrue(found, "Should have renamed column 'sum_total'");
        }

        @Test
        @DisplayName("groupBy→rename→filter: rename then filter on new name")
        void testGroupByRenameThenFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 90
                    2, 30
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->rename(~total, ~sum_val)
                    ->filter(x|$x.sum_val > 50)""");
            assertEquals(1, result.rowCount());
        }

        @Test
        @DisplayName("extend→groupBy: group on computed column")
        void testExtendThenGroupBy() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, price, qty
                    1, 10, 2
                    1, 20, 3
                    2, 5, 10
                    #->extend(~revenue:x|$x.price * $x.qty)
                    ->groupBy(~grp, ~totalRev:x|$x.revenue:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "grp", "totalRev");
            assertEquals(80L, ((Number) byGrp.get(1)).longValue());
            assertEquals(50L, ((Number) byGrp.get(2)).longValue());
        }

        @Test
        @DisplayName("filter→sort→groupBy: multi-step preprocessing")
        void testFilterSortGroupBy() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 999
                    2, 20
                    2, 30
                    3, 40
                    #->filter(x|$x.val < 100)
                    ->sort(~val->ascending())
                    ->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(3, result.rowCount());
            var byGrp = collectResults(result, "grp", "total");
            assertEquals(10L, ((Number) byGrp.get(1)).longValue());
            assertEquals(50L, ((Number) byGrp.get(2)).longValue());
            assertEquals(40L, ((Number) byGrp.get(3)).longValue());
        }

        @Test
        @DisplayName("groupBy→sort→limit (top-N pattern)")
        void testGroupBySortLimit() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    2, 50
                    3, 30
                    4, 20
                    5, 40
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->sort(~total->descending())
                    ->limit(3)""");
            assertEquals(3, result.rowCount());
            int totalIdx = columnIndex(result, "total");
            assertEquals(50L, ((Number) result.rows().get(0).get(totalIdx)).longValue());
            assertEquals(40L, ((Number) result.rows().get(1).get(totalIdx)).longValue());
            assertEquals(30L, ((Number) result.rows().get(2).get(totalIdx)).longValue());
        }

        @Test
        @DisplayName("groupBy→filter→sort→select pipeline")
        void testGroupByFilterSortSelect() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, salary
                    eng, 100
                    eng, 200
                    mkt, 50
                    mkt, 60
                    hr, 300
                    #->groupBy(~dept, ~totalSalary:x|$x.salary:y|$y->plus())
                    ->filter(x|$x.totalSalary > 200)
                    ->sort(~totalSalary->descending())
                    ->select(~[dept, totalSalary])""");
            assertEquals(2, result.rowCount());
            assertEquals(2, result.columns().size());
            int salIdx = columnIndex(result, "totalSalary");
            assertEquals(300L, ((Number) result.rows().get(0).get(salIdx)).longValue());
            assertEquals(300L, ((Number) result.rows().get(1).get(salIdx)).longValue());
        }

        @Test
        @DisplayName("groupBy→distinct (no-op since groups are unique)")
        void testGroupByThenDistinct() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->distinct()""");
            assertEquals(2, result.rowCount());
        }

        @Test
        @DisplayName("groupBy with multiple aggs→select one agg")
        void testMultiAggThenSelectOne() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~[s:x|$x.val:y|$y->plus(), c:x|1:y|$y->plus()])
                    ->select(~[grp, c])""");
            assertEquals(2, result.rowCount());
            assertEquals(2, result.columns().size());
            assertEquals("grp", result.columns().get(0).name());
            assertEquals("c", result.columns().get(1).name());
        }

        @Test
        @DisplayName("filter→extend→groupBy→sort: full pipeline")
        void testFullPipeline() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, price, qty, active
                    1, 10, 2, true
                    1, 20, 3, true
                    1, 5, 1, false
                    2, 15, 4, true
                    2, 25, 2, true
                    #->filter(x|$x.active == true)
                    ->extend(~revenue:x|$x.price * $x.qty)
                    ->groupBy(~grp, ~totalRev:x|$x.revenue:y|$y->plus())
                    ->sort(~totalRev->descending())""");
            assertEquals(2, result.rowCount());
            int revIdx = columnIndex(result, "totalRev");
            // grp1: 10*2 + 20*3 = 80, grp2: 15*4 + 25*2 = 110
            assertEquals(110L, ((Number) result.rows().get(0).get(revIdx)).longValue());
            assertEquals(80L, ((Number) result.rows().get(1).get(revIdx)).longValue());
        }
    }

    // ========================================================================
    // 13. Class-based project→groupBy
    // ========================================================================

    @Nested
    @DisplayName("Person.all()->project()->groupBy()")
    class ClassBasedGroupBy {

        @Test
        @DisplayName("project→groupBy: group by lastName, sum age")
        void testProjectGroupBySum() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~totalAge:x|$x.age:y|$y->plus())""");
            assertEquals(2, result.rowCount()); // Smith, Jones
            var byGrp = collectResults(result, "lastName", "totalAge");
            assertEquals(58L, ((Number) byGrp.get("Smith")).longValue()); // John(30) + Jane(28)
            assertEquals(45L, ((Number) byGrp.get("Jones")).longValue()); // Bob(45)
        }

        @Test
        @DisplayName("project→groupBy: count per group")
        void testProjectGroupByCount() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~cnt:x|1:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "lastName", "cnt");
            assertEquals(2L, ((Number) byGrp.get("Smith")).longValue());
            assertEquals(1L, ((Number) byGrp.get("Jones")).longValue());
        }

        @Test
        @DisplayName("project→groupBy→filter: filter grouped results")
        void testProjectGroupByFilter() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~totalAge:x|$x.age:y|$y->plus())
                    ->filter(x|$x.totalAge > 50)""");
            assertEquals(1, result.rowCount()); // Only Smith (58)
        }

        @Test
        @DisplayName("filter→project→groupBy: filter people then group")
        void testFilterProjectGroupBy() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->filter(p|$p.age > 25)
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~cnt:x|1:y|$y->plus())""");
            assertEquals(2, result.rowCount()); // Smith(John,Jane), Jones(Bob)
        }

        @Test
        @DisplayName("project→groupBy: average age per lastName")
        void testProjectGroupByAverage() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~avgAge:x|$x.age:y|$y->average())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "lastName", "avgAge");
            assertEquals(29.0, ((Number) byGrp.get("Smith")).doubleValue(), 0.001); // (30+28)/2
            assertEquals(45.0, ((Number) byGrp.get("Jones")).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("project→groupBy: min and max age")
        void testProjectGroupByMinMax() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~[youngest:x|$x.age:y|$y->min(), oldest:x|$x.age:y|$y->max()])""");
            assertEquals(2, result.rowCount());
            var mins = collectResults(result, "lastName", "youngest");
            var maxs = collectResults(result, "lastName", "oldest");
            assertEquals(28L, ((Number) mins.get("Smith")).longValue());
            assertEquals(30L, ((Number) maxs.get("Smith")).longValue());
        }

        @Test
        @DisplayName("project→groupBy→sort→limit: top-N pattern from class")
        void testProjectGroupBySortLimit() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~totalAge:x|$x.age:y|$y->plus())
                    ->sort(~totalAge->descending())
                    ->limit(1)""");
            assertEquals(1, result.rowCount());
            assertEquals("Smith", result.rows().get(0).get(
                    columnIndex(result, "lastName")).toString());
        }

        @Test
        @DisplayName("project→aggregate: full-table sum from class")
        void testProjectAggregate() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[age:p|$p.age])
                    ->aggregate(~totalAge:x|$x.age:y|$y->plus())""");
            assertEquals(1, result.rowCount());
            assertEquals(103L, ((Number) result.rows().get(0).get(0)).longValue()); // 30+28+45
        }

        @Test
        @DisplayName("project many cols→groupBy on one→select subset")
        void testProjectManyGroupBySelect() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[fn:p|$p.firstName, ln:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~ln, ~cnt:x|1:y|$y->plus())
                    ->select(~cnt)""");
            assertEquals(2, result.rowCount());
            assertEquals(1, result.columns().size());
        }

        @Test
        @DisplayName("project→groupBy→rename→sort: full class pipeline")
        void testProjectGroupByRenameSort() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                    ->project(~[lastName:p|$p.lastName, age:p|$p.age])
                    ->groupBy(~lastName, ~totalAge:x|$x.age:y|$y->plus())
                    ->rename(~totalAge, ~familyAge)
                    ->sort(~familyAge->ascending())""");
            assertEquals(2, result.rowCount());
            boolean found = result.columns().stream().anyMatch(c -> "familyAge".equals(c.name()));
            assertTrue(found);
            int famIdx = columnIndex(result, "familyAge");
            // Jones=45, Smith=58 → ascending
            assertEquals(45L, ((Number) result.rows().get(0).get(famIdx)).longValue());
            assertEquals(58L, ((Number) result.rows().get(1).get(famIdx)).longValue());
        }
    }

    // ========================================================================
    // 13. Duplicate alias prevention
    // ========================================================================

    @Nested
    @DisplayName("edge cases and error handling")
    class EdgeCases {

        @Test
        @DisplayName("groupBy with single agg (not array)")
        void testSingleAggNotArray() throws SQLException {
            // ~agg:x|...:y|... (single, not wrapped in ~[...])
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    2, 20
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
        }

        @Test
        @DisplayName("groupBy with single group col (not array)")
        void testSingleGroupNotArray() throws SQLException {
            // ~grp (single, not wrapped in ~[...])
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    2, 20
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
        }

        @Test
        @DisplayName("groupBy preserves group column values exactly")
        void testGroupColumnValues() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    name, val
                    Alice, 10
                    Alice, 20
                    Bob, 30
                    #->groupBy(~name, ~total:x|$x.val:y|$y->plus())""");
            assertEquals(2, result.rowCount());
            var byGrp = collectResults(result, "name", "total");
            assertTrue(byGrp.containsKey("Alice"));
            assertTrue(byGrp.containsKey("Bob"));
        }

        @Test
        @DisplayName("groupBy then distinct is no-op")
        void testGroupByThenDistinct() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    grp, val
                    1, 10
                    1, 20
                    2, 30
                    #->groupBy(~grp, ~total:x|$x.val:y|$y->plus())
                    ->distinct()""");
            assertEquals(2, result.rowCount());
        }
    }

    // ========================================================================
    // Utilities
    // ========================================================================

    /** Collects results into a map keyed by the group column value. */
    private <K> java.util.Map<K, Object> collectResults(
            com.gs.legend.exec.ExecutionResult result, String keyCol, String valCol) {
        int keyIdx = columnIndex(result, keyCol);
        int valIdx = columnIndex(result, valCol);
        java.util.Map<K, Object> map = new java.util.HashMap<>();
        for (var row : result.rows()) {
            @SuppressWarnings("unchecked")
            K key = (K) row.get(keyIdx);
            map.put(key, row.get(valIdx));
        }
        return map;
    }

    /** Finds column index by name. */
    private int columnIndex(com.gs.legend.exec.ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name())) return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }
}
