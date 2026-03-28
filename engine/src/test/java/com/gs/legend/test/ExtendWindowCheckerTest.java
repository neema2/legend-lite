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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for window functions via extend() + over().
 *
 * Uses #TDS inline data — same approach as ExtendCheckerTest and
 * DuckDBIntegrationTest.
 *
 * All patterns are taken directly from proven DuckDBIntegrationTest examples:
 * - Standalone over(): extend(over(~grp), ~col:{p,w,r|$r.field}:y|$y->func())
 * - Chaining over(): extend(~grp->over(~sort->desc()),
 * ~col:{p,w,r|$p->func($r).field})
 * - Frame spec: over(~grp, ~sort->asc(), unbounded()->rows(unbounded()))
 * - No-window agg: extend(~col:c|$c.field:y|$y->func())
 */
public class ExtendWindowCheckerTest extends AbstractDatabaseTest {

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
        if (connection != null)
            connection.close();
    }

    // ==================== Common TDS data ====================

    /** 10-row TDS matching DuckDBIntegrationTest's standard dataset */
    private static final String TDS10 = """
            |#TDS
                id, grp, name
                1, 2, A
                2, 1, B
                3, 3, C
                4, 4, D
                5, 2, E
                6, 1, F
                7, 3, G
                8, 1, H
                9, 5, I
                10, 0, J
            #""";

    /** 5-row TDS with integer partition, order, value */
    private static final String NUMS = """
            |#TDS
                p, o, v
                0, 1, 10
                0, 2, 20
                0, 3, 30
                1, 1, 100
                1, 2, 200
            #""";

    // ==================== Utilities ====================

    private int colIdx(ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name()))
                return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }

    // ========================================================================
    // 1. Window aggregates — fn1+fn2 with standalone over()
    // Pattern: extend(over(~partition), ~col:{p,w,r|$r.field}:y|$y->func())
    // From DuckDBIntegrationTest line 3475
    // ========================================================================

    @Nested
    @DisplayName("Window aggregates — fn1+fn2 with standalone over()")
    class WindowAggregates {

        @Test
        @DisplayName("sum via plus() — partition by grp")
        void testSum() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~newCol:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // grp=0: id=10 → sum=10; grp=1: id=2+6+8 → sum=16; grp=2: id=1+5 → sum=6
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int newCol = ((Number) row.get(3)).intValue();
                int expected = switch (grp) {
                    case 0 -> 10;
                    case 1 -> 16;
                    case 2 -> 6;
                    case 3 -> 10;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, newCol, "Sum for grp=" + grp);
            }
        }

        @Test
        @DisplayName("count via size() — partition by grp")
        void testCount() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~cnt:{p,w,r|$r.id}:y|$y->size())");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int cnt = ((Number) row.get(3)).intValue();
                int expected = switch (grp) {
                    case 0, 4, 5 -> 1;
                    case 2, 3 -> 2;
                    case 1 -> 3;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, cnt, "Count for grp=" + grp);
            }
        }

        @Test
        @DisplayName("max via max() — partition by grp")
        void testMax() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~mx:{p,w,r|$r.id}:y|$y->max())");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int mx = ((Number) row.get(3)).intValue();
                int expected = switch (grp) {
                    case 0 -> 10;
                    case 1 -> 8;
                    case 2 -> 5;
                    case 3 -> 7;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, mx, "Max for grp=" + grp);
            }
        }

        @Test
        @DisplayName("min via min() — partition by grp")
        void testMin() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~mn:{p,w,r|$r.id}:y|$y->min())");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int mn = ((Number) row.get(3)).intValue();
                int expected = switch (grp) {
                    case 0 -> 10;
                    case 1 -> 2;
                    case 2 -> 1;
                    case 3 -> 3;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, mn, "Min for grp=" + grp);
            }
        }

        @Test
        @DisplayName("average via average() — partition by grp")
        void testAverage() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~avg:{p,w,r|$r.id}:y|$y->average())");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                double avg = ((Number) row.get(3)).doubleValue();
                // grp=0: 10/1=10.0, grp=1: 16/3≈5.33, grp=2: 6/2=3.0, grp=3: 10/2=5.0, grp=4:
                // 4/1=4.0, grp=5: 9/1=9.0
                double expected = switch (grp) {
                    case 0 -> 10.0;
                    case 1 -> 16.0 / 3;
                    case 2 -> 3.0;
                    case 3 -> 5.0;
                    case 4 -> 4.0;
                    case 5 -> 9.0;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, avg, 0.01, "Avg for grp=" + grp);
            }
        }

        @Test
        @DisplayName("sum with partition+sort — from NUMS")
        void testSumWithPartitionAndSort() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending()), ~total:{p,w,r|$r.v}:y|$y->plus())");
            assertEquals(5, r.rowCount());
            // Running sum: p=0: [10, 30, 60], p=1: [100, 300]
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int o = ((Number) row.get(1)).intValue();
                int total = ((Number) row.get(3)).intValue();
                if (p == 0) {
                    int expected = switch (o) {
                        case 1 -> 10;
                        case 2 -> 30;
                        case 3 -> 60;
                        default -> -1;
                    };
                    assertEquals(expected, total, "Running sum p=0,o=" + o);
                } else {
                    int expected = switch (o) {
                        case 1 -> 100;
                        case 2 -> 300;
                        default -> -1;
                    };
                    assertEquals(expected, total, "Running sum p=1,o=" + o);
                }
            }
        }

        @Test
        @DisplayName("joinStrings with partition+sort+frame — exact PCT pattern")
        void testJoinStringsWithFrame() throws SQLException {
            // Exact pattern from
            // DuckDBIntegrationTest.testOLAPAggStringWithPartitionAndOrderASCUnboundedWindow
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending(), unbounded()->rows(unbounded())), ~newCol:{p,w,r|$r.name}:y|$y->joinStrings(''))");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                String newCol = (String) row.get(3);
                String expected = switch (grp) {
                    case 0 -> "J";
                    case 1 -> "BFH";
                    case 2 -> "AE";
                    case 3 -> "CG";
                    case 4 -> "D";
                    case 5 -> "I";
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, newCol, "For grp=" + grp);
            }
        }

        @Test
        @DisplayName("multiple window agg columns — exact PCT pattern")
        void testMultipleWindowAggColumns() throws SQLException {
            // From
            // DuckDBIntegrationTest.testOLAPAggWithPartitionAndOrderUnboundedWindowMultipleColumns
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->descending(), unbounded()->rows(unbounded())), ~[newCol:{p,w,r|$r.name}:y|$y->joinStrings(''),other:{p,w,r|$r.id}:y|$y->plus()])");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                String newCol = (String) row.get(3);
                int other = ((Number) row.get(4)).intValue();
                if (grp == 1) {
                    assertEquals("HFB", newCol);
                    assertEquals(16, other);
                }
            }
        }

        @Test
        @DisplayName("multiple sort columns — exact PCT pattern")
        void testMultipleSortColumns() throws SQLException {
            // From
            // DuckDBIntegrationTest.testOLAPWithPartitionAndMultipleOrderWindowMultipleColumnsWithFilter
            var r = executeRelation(
                    """
                            |#TDS
                                id, grp, name, height
                                1, 2, A, 11
                                2, 1, B, 12
                                3, 3, C, 12
                                4, 3, B, 11
                                5, 2, B, 13
                                6, 1, C, 12
                                7, 3, A, 13
                                8, 1, B, 14
                            #->extend(over(~grp, [~name->ascending(), ~height->ascending()]), ~newCol:{p,w,r|$r.id}:y|$y->plus())""");
            assertEquals(8, r.rowCount());
            // grp=1 (B12=2, B14=8, C12=6 sorted by name asc, height asc): running sums [2,
            // 10, 16]
            // grp=2 (A11=1, B13=5 sorted): running sums [1, 6]
            // grp=3 (A13=7, B11=4, C12=3 sorted): running sums [7, 11, 14]
            for (var row : r.rows()) {
                int id = ((Number) row.get(0)).intValue();
                int newCol = ((Number) row.get(4)).intValue();
                int expected = switch (id) {
                    case 1 -> 1; // grp=2, A11 first
                    case 2 -> 2; // grp=1, B12 first
                    case 5 -> 6; // grp=2, B13 second: 1+5
                    case 6 -> 16; // grp=1, C12 third: 2+8+6
                    case 7 -> 7; // grp=3, A13 first
                    case 8 -> 10; // grp=1, B14 second: 2+8
                    case 4 -> 11; // grp=3, B11 second: 7+4
                    case 3 -> 14; // grp=3, C12 third: 7+4+3
                    default -> -1;
                };
                assertEquals(expected, newCol, "newCol for id=" + id);
            }
        }

        @Test
        @DisplayName("sum preserves original columns")
        void testSumPreservesColumns() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~total:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(4, r.columns().size());
            assertEquals("id", r.columns().get(0).name());
            assertEquals("grp", r.columns().get(1).name());
            assertEquals("name", r.columns().get(2).name());
            assertEquals("total", r.columns().get(3).name());
        }
    }

    // ========================================================================
    // 2. No-window aggregates — fn1+fn2 without over()
    // Pattern: extend(~col:c|$c.field:y|$y->func())
    // From DuckDBIntegrationTest line 3371
    // ========================================================================

    @Nested
    @DisplayName("No-window aggregates — fn1+fn2 without over()")
    class NoWindowAggregates {

        @Test
        @DisplayName("sum entire relation via plus()")
        void testSumAll() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~[newCol:c|$c.id:y|$y->plus(), other:c|$c.grp:y|$y->plus()])");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                assertEquals(55, ((Number) row.get(3)).intValue(), "newCol=sum(id)=55");
                assertEquals(22, ((Number) row.get(4)).intValue(), "other=sum(grp)=22");
            }
        }

        @Test
        @DisplayName("average entire relation")
        void testAvgAll() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~avg:c|$c.id:y|$y->average())");
            for (var row : r.rows()) {
                assertEquals(5.5, ((Number) row.get(3)).doubleValue(), 0.01);
            }
        }

        @Test
        @DisplayName("count entire relation")
        void testCountAll() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~cnt:c|$c.id:y|$y->size())");
            for (var row : r.rows()) {
                assertEquals(10, ((Number) row.get(3)).intValue());
            }
        }

        @Test
        @DisplayName("min entire relation")
        void testMinAll() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~mn:c|$c.id:y|$y->min())");
            for (var row : r.rows()) {
                assertEquals(1, ((Number) row.get(3)).intValue());
            }
        }

        @Test
        @DisplayName("max entire relation")
        void testMaxAll() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~mx:c|$c.id:y|$y->max())");
            for (var row : r.rows()) {
                assertEquals(10, ((Number) row.get(3)).intValue());
            }
        }

        @Test
        @DisplayName("preserves original columns")
        void testPreservesColumns() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~total:c|$c.id:y|$y->plus())");
            assertEquals(4, r.columns().size());
            assertEquals("total", r.columns().get(3).name());
        }
    }

    // ========================================================================
    // 3. Ranking functions — chaining over()
    // Pattern: extend(~grp->over(~sort->desc()), ~col:{p,w,r|$p->func($r)})
    // 2-param: rowNumber, ntile
    // ========================================================================

    @Nested
    @DisplayName("Ranking window functions")
    class RankingFunctions {

        @Test
        @DisplayName("rowNumber — desc sort")
        void testRowNumber() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "rn".equals(c.name())));
            for (var row : r.rows()) {
                long rn = ((Number) row.get(colIdx(r, "rn"))).longValue();
                assertTrue(rn >= 1, "Row number should be >= 1");
            }
        }

        @Test
        @DisplayName("rowNumber — ascending sort")
        void testRowNumberAsc() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                long rn = ((Number) row.get(colIdx(r, "rn"))).longValue();
                assertTrue(rn >= 1, "Row number should be >= 1");
            }
        }

        @Test
        @DisplayName("ntile(2) — split into 2 buckets")
        void testNtile2() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~bucket:{p,w,r|$p->ntile($r,2)})");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int bucket = ((Number) row.get(colIdx(r, "bucket"))).intValue();
                assertTrue(bucket == 1 || bucket == 2, "Bucket should be 1 or 2");
            }
        }

        @Test
        @DisplayName("ntile(3)")
        void testNtile3() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~bucket:{p,w,r|$p->ntile($r,3)})");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int bucket = ((Number) row.get(colIdx(r, "bucket"))).intValue();
                assertTrue(bucket >= 1 && bucket <= 3, "Bucket should be 1, 2, or 3 but was " + bucket);
            }
        }

        @Test
        @DisplayName("rowNumber — single-element partition")
        void testRowNumberSinglePartition() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            // grp=0 (id=10), grp=4 (id=4), grp=5 (id=9) each have 1 row ⇒ rn=1
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                if (grp == 0 || grp == 4 || grp == 5) {
                    assertEquals(1, ((Number) row.get(colIdx(r, "rn"))).intValue(),
                            "Single-element partition grp=" + grp + " should have rn=1");
                }
            }
        }

        @Test
        @DisplayName("rowNumber preserves all columns")
        void testRowNumberPreservesColumns() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            assertEquals(4, r.columns().size());
            assertEquals("rn", r.columns().get(3).name());
        }

        @Test
        @DisplayName("rowNumber on NUMS")
        void testRowNumberNums() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~p->over(~o->ascending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            assertEquals(5, r.rowCount());
            // p=0 has 3 rows → rn 1,2,3; p=1 has 2 rows → rn 1,2
            for (var row : r.rows()) {
                int o = ((Number) row.get(1)).intValue();
                int rn = ((Number) row.get(colIdx(r, "rn"))).intValue();
                assertEquals(o, rn, "Row number should match order column");
            }
        }
    }

    // ========================================================================
    // 4. Offset/value window functions — chaining over()
    // Pattern: extend(~grp->over(~sort->desc()), ~col:{p,w,r|$p->func($r).field})
    // From DuckDBIntegrationTest lines 3787, 5313
    // ========================================================================

    @Nested
    @DisplayName("Offset window functions")
    class OffsetFunctions {

        @Test
        @DisplayName("nth value — 2nd row by id desc")
        void testNth() throws SQLException {
            // Exact pattern from DuckDBIntegrationTest.testNthValueWindowFunction
            var r = executeRelation("""
                    |#TDS
                        id, grp, name
                        1, 2, A
                        2, 1, B
                        3, 3, C
                        4, 4, D
                        5, 2, E
                    #->extend(~grp->over(~id->descending()), ~newCol:{p,w,r|$p->nth($w, $r, 2).id})""");
            assertEquals(5, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "newCol".equals(c.name())));
        }

        @Test
        @DisplayName("lead — next row's id")
        void testLead() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~next:{p,w,r|$p->lead($r).id})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "next".equals(c.name())));
        }

        @Test
        @DisplayName("lag — previous row's id")
        void testLag() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~prev:{p,w,r|$p->lag($r).id})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "prev".equals(c.name())));
        }

        @Test
        @DisplayName("lead returns null for last row in partition")
        void testLeadLastRow() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~next:{p,w,r|$p->lead($r).id})");
            // At least some rows should have null lead (last in partition)
            boolean foundNull = r.rows().stream()
                    .anyMatch(row -> row.get(colIdx(r, "next")) == null);
            assertTrue(foundNull, "Last row in each partition should have null lead");
        }

        @Test
        @DisplayName("lag returns null for first row in partition")
        void testLagFirstRow() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~prev:{p,w,r|$p->lag($r).id})");
            boolean foundNull = r.rows().stream()
                    .anyMatch(row -> row.get(colIdx(r, "prev")) == null);
            assertTrue(foundNull, "First row in each partition should have null lag");
        }

        @Test
        @DisplayName("first value with window")
        void testFirst() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~fst:{p,w,r|$p->first($w,$r).name})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "fst".equals(c.name())));
        }

        @Test
        @DisplayName("nth on NUMS dataset")
        void testNthNums() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~p->over(~o->ascending()), ~nth:{p,w,r|$p->nth($w, $r, 1).v})");
            assertEquals(5, r.rowCount());
            // nth(1) returns the 1st row's v in the window frame
            // p=0 (o=1,2,3): nth(1)=10 for all; p=1 (o=1,2): nth(1)=100 for all
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int nth = ((Number) row.get(3)).intValue();
                if (p == 0)
                    assertEquals(10, nth, "nth(1) for p=0 should be first value");
                else
                    assertEquals(100, nth, "nth(1) for p=1 should be first value");
            }
        }
    }

    // ========================================================================
    // 5. Over clause variants
    // ========================================================================

    @Nested
    @DisplayName("Over clause variants")
    class OverClauseVariants {

        @Test
        @DisplayName("standalone over — partition only")
        void testPartitionOnly() throws SQLException {
            // From DuckDBIntegrationTest line 3475
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~total:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // grp=1: sum(2,6,8)=16, grp=2: sum(1,5)=6
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int total = ((Number) row.get(3)).intValue();
                if (grp == 1)
                    assertEquals(16, total);
                if (grp == 2)
                    assertEquals(6, total);
            }
        }

        @Test
        @DisplayName("standalone over — partition + sort")
        void testPartitionAndSort() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~total:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // Running sum within partition: grp=1 (ids 2,6,8 asc): [2, 8, 16]
            var grp1Rows = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .sorted((a, b) -> Integer.compare(((Number) a.get(0)).intValue(), ((Number) b.get(0)).intValue()))
                    .toList();
            assertEquals(2, ((Number) grp1Rows.get(0).get(3)).intValue());
            assertEquals(8, ((Number) grp1Rows.get(1).get(3)).intValue());
            assertEquals(16, ((Number) grp1Rows.get(2).get(3)).intValue());
        }

        @Test
        @DisplayName("standalone over — partition + sort + frame")
        void testPartitionSortFrame() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending(), unbounded()->rows(unbounded())), ~total:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // Unbounded frame = full partition sum: grp=1 sum=16, grp=2 sum=6
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int total = ((Number) row.get(3)).intValue();
                if (grp == 1)
                    assertEquals(16, total);
                if (grp == 2)
                    assertEquals(6, total);
            }
        }

        @Test
        @DisplayName("chaining over — partition + sort for ranking")
        void testChainingPartAndSort() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            assertEquals(10, r.rowCount());
            // Each partition gets row numbers 1..N descending by id
            for (var row : r.rows()) {
                long rn = ((Number) row.get(colIdx(r, "rn"))).longValue();
                assertTrue(rn >= 1, "Row number should be >= 1");
            }
        }

        @Test
        @DisplayName("chaining over — no sort for aggregate")
        void testChainingPartOnly() throws SQLException {
            // From DuckDBIntegrationTest line 4546
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(), ~newCol:{p,w,r|$r.id}:y|$y->size())");
            assertEquals(10, r.rowCount());
            // grp=1 has 3 rows
            var grp1Row = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .findFirst().orElseThrow();
            assertEquals(3, ((Number) grp1Row.get(3)).intValue());
        }

        @Test
        @DisplayName("extend in pipeline: filter → window")
        void testFilterThenWindow() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->filter(x | $x.grp > 2)" +
                    "->extend(over(~grp), ~total:{p,w,r|$r.id}:y|$y->plus())");
            // grp>2: grp=3(ids 3,7), grp=4(id 4), grp=5(id 9) → 4 rows
            assertEquals(4, r.rowCount());
            // grp=3: sum(3,7)=10, grp=4: sum(4)=4, grp=5: sum(9)=9
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int total = ((Number) row.get(3)).intValue();
                int expected = switch (grp) {
                    case 3 -> 10;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> -1;
                };
                assertEquals(expected, total, "total for grp=" + grp);
            }
        }

        @Test
        @DisplayName("extend in pipeline: window → filter")
        void testWindowThenFilter() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})" +
                    "->filter(x | $x.rn == 1)");
            // 6 partitions → 6 rows with rn=1
            assertEquals(6, r.rowCount());
            for (var row : r.rows()) {
                assertEquals(1, ((Number) row.get(colIdx(r, "rn"))).intValue());
            }
        }

        @Test
        @DisplayName("extend in pipeline: window → sort")
        void testWindowThenSort() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})" +
                    "->sort(ascending(~rn))");
            assertEquals(10, r.rowCount());
            // After sort by rn ascending, first rows should have rn=1
            int firstRn = ((Number) r.rows().get(0).get(colIdx(r, "rn"))).intValue();
            assertEquals(1, firstRn, "First row after sort should have rn=1");
        }
    }

    // ========================================================================
    // 6. Frame specifications
    // Frame constructor: unbounded()->rows(N) or N->rows(M)
    // From DuckDBIntegrationTest line 3587
    // ========================================================================

    @Nested
    @DisplayName("Frame specifications")
    class FrameSpecs {

        @Test
        @DisplayName("unbounded frame — entire partition")
        void testUnboundedFrame() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), unbounded()->rows(unbounded())), ~total:{p,w,r|$r.v}:y|$y->plus())");
            assertEquals(5, r.rowCount());
            // Every row in partition 0 should have sum=60, partition 1 sum=300
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int total = ((Number) row.get(3)).intValue();
                if (p == 0)
                    assertEquals(60, total);
                else
                    assertEquals(300, total);
            }
        }

        @Test
        @DisplayName("running sum — unbounded to current")
        void testRunningSumFrame() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), unbounded()->rows(0)), ~rs:{p,w,r|$r.v}:y|$y->plus())");
            assertEquals(5, r.rowCount());
            // Running sum: p=0: [10, 30, 60], p=1: [100, 300]
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int o = ((Number) row.get(1)).intValue();
                int rs = ((Number) row.get(3)).intValue();
                if (p == 0) {
                    int expected = switch (o) {
                        case 1 -> 10;
                        case 2 -> 30;
                        case 3 -> 60;
                        default -> -1;
                    };
                    assertEquals(expected, rs, "Running sum p=0,o=" + o);
                } else {
                    int expected = switch (o) {
                        case 1 -> 100;
                        case 2 -> 300;
                        default -> -1;
                    };
                    assertEquals(expected, rs, "Running sum p=1,o=" + o);
                }
            }
        }

        @Test
        @DisplayName("sliding window — previous 1 to next 1")
        void testSlidingFrame() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), (-1)->rows(1)), ~avg3:{p,w,r|$r.v}:y|$y->average())");
            assertEquals(5, r.rowCount());
            // Sliding avg (-1,+1): p=0: o=1→avg(10,20)=15, o=2→avg(10,20,30)=20,
            // o=3→avg(20,30)=25
            // p=1: o=1→avg(100,200)=150, o=2→avg(100,200)=150
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int o = ((Number) row.get(1)).intValue();
                double avg3 = ((Number) row.get(3)).doubleValue();
                if (p == 0 && o == 2)
                    assertEquals(20.0, avg3, 0.01, "Middle of p=0");
                if (p == 1)
                    assertEquals(150.0, avg3, 0.01, "p=1,o=" + o);
            }
        }

        @Test
        @DisplayName("current row only — 0 to 0")
        void testCurrentRowFrame() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), 0->rows(0)), ~cur:{p,w,r|$r.v}:y|$y->plus())");
            // sum of just current row = current value
            for (var row : r.rows()) {
                int v = ((Number) row.get(2)).intValue();
                int cur = ((Number) row.get(3)).intValue();
                assertEquals(v, cur, "Current row frame sum should equal value itself");
            }
        }

        @Test
        @DisplayName("running min — unbounded to current")
        void testRunningMin() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), unbounded()->rows(0)), ~runMin:{p,w,r|$r.v}:y|$y->min())");
            // Running min in each partition should be the first value (ascending order)
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int runMin = ((Number) row.get(3)).intValue();
                if (p == 0)
                    assertEquals(10, runMin);
                else
                    assertEquals(100, runMin);
            }
        }

        @Test
        @DisplayName("running max — unbounded to current")
        void testRunningMax() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), unbounded()->rows(0)), ~runMax:{p,w,r|$r.v}:y|$y->max())");
            assertEquals(5, r.rowCount());
            // Running max: p=0: [10, 20, 30], p=1: [100, 200]
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int o = ((Number) row.get(1)).intValue();
                int runMax = ((Number) row.get(3)).intValue();
                if (p == 0)
                    assertEquals(o * 10, runMax, "Running max p=0,o=" + o);
                else
                    assertEquals(o * 100, runMax, "Running max p=1,o=" + o);
            }
        }

        @Test
        @DisplayName("running count — unbounded to current")
        void testRunningCount() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), unbounded()->rows(0)), ~cnt:{p,w,r|$r.v}:y|$y->size())");
            assertEquals(5, r.rowCount());
            // Running count: p=0: [1, 2, 3], p=1: [1, 2]
            for (var row : r.rows()) {
                int o = ((Number) row.get(1)).intValue();
                int cnt = ((Number) row.get(3)).intValue();
                assertEquals(o, cnt, "Running count should match order column");
            }
        }

        @Test
        @DisplayName("unbounded frame with TDS10")
        void testUnboundedFrameTDS10() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending(), unbounded()->rows(unbounded())), ~total:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int total = ((Number) row.get(3)).intValue();
                if (grp == 1)
                    assertEquals(16, total, "grp=1 total should be 16");
            }
        }

        @Test
        @DisplayName("running sum with TDS10")
        void testRunningSumTDS10() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending(), unbounded()->rows(0)), ~rs:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // Spot-check grp=1 (ids 2,6,8 ascending): running sums [2, 8, 16]
            var grp1Rows = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .sorted((a, b) -> Integer.compare(((Number) a.get(0)).intValue(), ((Number) b.get(0)).intValue()))
                    .toList();
            assertEquals(3, grp1Rows.size());
            assertEquals(2, ((Number) grp1Rows.get(0).get(3)).intValue());
            assertEquals(8, ((Number) grp1Rows.get(1).get(3)).intValue());
            assertEquals(16, ((Number) grp1Rows.get(2).get(3)).intValue());
        }
    }

    // ========================================================================
    // 8. Additional ranking functions — rank, denseRank, percentRank, cumeDist
    // ========================================================================

    @Nested
    @DisplayName("Additional ranking functions")
    class AdditionalRankingFunctions {

        @Test
        @DisplayName("rank — partition by grp, order by id desc")
        void testRank() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rnk:{p,w,r|$p->rank($w,$r)})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "rnk".equals(c.name())));
            for (var row : r.rows()) {
                long rnk = ((Number) row.get(colIdx(r, "rnk"))).longValue();
                assertTrue(rnk >= 1, "Rank should be >= 1");
            }
        }

        @Test
        @DisplayName("denseRank — partition by grp, order by id desc")
        void testDenseRank() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~drnk:{p,w,r|$p->denseRank($w,$r)})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "drnk".equals(c.name())));
            for (var row : r.rows()) {
                long drnk = ((Number) row.get(colIdx(r, "drnk"))).longValue();
                assertTrue(drnk >= 1, "Dense rank should be >= 1");
            }
        }

        @Test
        @DisplayName("percentRank — partition by grp, order by id desc")
        void testPercentRank() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~prnk:{p,w,r|$p->percentRank($w,$r)})");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                double prnk = ((Number) row.get(colIdx(r, "prnk"))).doubleValue();
                assertTrue(prnk >= 0 && prnk <= 1, "Percent rank should be between 0 and 1");
            }
        }

        @Test
        @DisplayName("cumulativeDistribution — partition by grp, order by id desc")
        void testCumeDist() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~cdist:{p,w,r|$p->cumulativeDistribution($w,$r)})");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                double cdist = ((Number) row.get(colIdx(r, "cdist"))).doubleValue();
                assertTrue(cdist > 0 && cdist <= 1, "Cumulative distribution should be > 0 and <= 1");
            }
        }

        @Test
        @DisplayName("rank on ascending sort")
        void testRankAsc() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~rnk:{p,w,r|$p->rank($w,$r)})");
            assertEquals(10, r.rowCount());
            for (var row : r.rows()) {
                long rnk = ((Number) row.get(colIdx(r, "rnk"))).longValue();
                assertTrue(rnk >= 1, "Rank should be >= 1");
            }
        }

        @Test
        @DisplayName("denseRank on ascending sort on NUMS")
        void testDenseRankNums() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~p->over(~o->ascending()), ~drnk:{p,w,r|$p->denseRank($w,$r)})");
            assertEquals(5, r.rowCount());
            // Each o value is unique within partition, so dense rank = row position
            for (var row : r.rows()) {
                int o = ((Number) row.get(1)).intValue();
                int drnk = ((Number) row.get(colIdx(r, "drnk"))).intValue();
                assertEquals(o, drnk, "Dense rank should match order column");
            }
        }

        @Test
        @DisplayName("rank preserves columns")
        void testRankPreservesColumns() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rnk:{p,w,r|$p->rank($w,$r)})");
            assertEquals(4, r.columns().size());
            assertEquals("rnk", r.columns().get(3).name());
        }
    }

    // ========================================================================
    // 9. Additional offset functions — last()
    // ========================================================================

    @Nested
    @DisplayName("Additional offset functions")
    class AdditionalOffsetFunctions {

        @Test
        @DisplayName("last value with window")
        void testLast() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~lst:{p,w,r|$p->last($w,$r).name})");
            assertEquals(10, r.rowCount());
            assertTrue(r.columns().stream().anyMatch(c -> "lst".equals(c.name())));
        }

        @Test
        @DisplayName("last on NUMS dataset")
        void testLastNums() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~p->over(~o->ascending()), ~lst:{p,w,r|$p->last($w,$r).v})");
            assertEquals(5, r.rowCount());
            // last() with ascending order returns the last row's v in the window
            for (var row : r.rows()) {
                int lst = ((Number) row.get(3)).intValue();
                // Default frame (unbounded preceding to current): last = current row value
                int v = ((Number) row.get(2)).intValue();
                assertEquals(v, lst, "last() with default frame should be current row's v");
            }
        }

        @Test
        @DisplayName("first and last in same extend via array")
        void testFirstAndLastArray() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~[fst:{p,w,r|$p->first($w,$r).name}, lst:{p,w,r|$p->last($w,$r).name}])");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
        }
    }

    // ========================================================================
    // 10. Window aggregates — order-only (no partition)
    // ========================================================================

    @Nested
    @DisplayName("Window agg — order-only (no partition)")
    class OrderOnlyWindowAgg {

        @Test
        @DisplayName("running sum via plus() — order by id ascending")
        void testRunningSumOrderOnly() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~id->ascending()), ~runSum:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // No partition → running sum over all rows ordered by id ascending
            // id=1: sum=1, id=2: sum=3, ..., id=N: sum=N*(N+1)/2
            for (var row : r.rows()) {
                int id = ((Number) row.get(0)).intValue();
                int runSum = ((Number) row.get(3)).intValue();
                assertEquals(id * (id + 1) / 2, runSum, "Running sum at id=" + id);
            }
        }

        @Test
        @DisplayName("running sum via plus() — order by id descending")
        void testRunningSumOrderDesc() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~id->descending()), ~runSum:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // Descending: id=10 first (sum=10), then id=9 (sum=19), ... id=1 (sum=55)
            for (var row : r.rows()) {
                int id = ((Number) row.get(0)).intValue();
                int runSum = ((Number) row.get(3)).intValue();
                // sum of ids from 10 down to id = 55 - id*(id-1)/2
                assertEquals(55 - id * (id - 1) / 2, runSum, "Running sum desc at id=" + id);
            }
        }

        @Test
        @DisplayName("running count via size() — order by id ascending")
        void testRunningCountOrderOnly() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~id->ascending()), ~cnt:{p,w,r|$r.id}:y|$y->size())");
            assertEquals(10, r.rowCount());
            // Running count ascending: at id=N, count should be N
            for (var row : r.rows()) {
                int id = ((Number) row.get(0)).intValue();
                int cnt = ((Number) row.get(3)).intValue();
                assertEquals(id, cnt, "Running count at id=" + id);
            }
        }
    }

    // ========================================================================
    // 11. Window aggregates — running aggregates (partition+sort, default frame)
    // ========================================================================

    @Nested
    @DisplayName("Window agg — running aggregates with partition+sort")
    class RunningAggregates {

        @Test
        @DisplayName("running sum — partition by grp, sort by id asc")
        void testRunningSum() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~runSum:{p,w,r|$r.id}:y|$y->plus())");
            assertEquals(10, r.rowCount());
            // Spot-check grp=1 (ids 2,6,8 asc): running sums [2, 8, 16]
            var grp1Rows = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .sorted((a, b) -> Integer.compare(((Number) a.get(0)).intValue(), ((Number) b.get(0)).intValue()))
                    .toList();
            assertEquals(2, ((Number) grp1Rows.get(0).get(3)).intValue());
            assertEquals(8, ((Number) grp1Rows.get(1).get(3)).intValue());
            assertEquals(16, ((Number) grp1Rows.get(2).get(3)).intValue());
        }

        @Test
        @DisplayName("running count — partition by grp, sort by id asc")
        void testRunningCount() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~runCnt:{p,w,r|$r.id}:y|$y->size())");
            assertEquals(10, r.rowCount());
            // Spot-check grp=1 (ids 2,6,8): running counts [1, 2, 3]
            var grp1Rows = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .sorted((a, b) -> Integer.compare(((Number) a.get(0)).intValue(), ((Number) b.get(0)).intValue()))
                    .toList();
            assertEquals(1, ((Number) grp1Rows.get(0).get(3)).intValue());
            assertEquals(2, ((Number) grp1Rows.get(1).get(3)).intValue());
            assertEquals(3, ((Number) grp1Rows.get(2).get(3)).intValue());
        }

        @Test
        @DisplayName("running min — partition by grp, sort by id asc")
        void testRunningMin() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~runMin:{p,w,r|$r.id}:y|$y->min())");
            assertEquals(10, r.rowCount());
            // Running min within each partition is always the first (smallest) id
            // grp=0: min=10, grp=1: min=2, grp=2: min=1, grp=3: min=3, grp=4: min=4, grp=5:
            // min=9
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int runMin = ((Number) row.get(3)).intValue();
                int expected = switch (grp) {
                    case 0 -> 10;
                    case 1 -> 2;
                    case 2 -> 1;
                    case 3 -> 3;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expected, runMin, "Running min for grp=" + grp);
            }
        }

        @Test
        @DisplayName("running max — partition by grp, sort by id asc")
        void testRunningMax() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~runMax:{p,w,r|$r.id}:y|$y->max())");
            assertEquals(10, r.rowCount());
            // Running max = current id (since sorted ascending, max is always the current
            // row)
            for (var row : r.rows()) {
                int id = ((Number) row.get(0)).intValue();
                int runMax = ((Number) row.get(3)).intValue();
                assertEquals(id, runMax, "Running max should be current id");
            }
        }

        @Test
        @DisplayName("running average — partition by grp, sort by id asc")
        void testRunningAvg() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~runAvg:{p,w,r|$r.id}:y|$y->average())");
            assertEquals(10, r.rowCount());
            // Spot-check grp=1 (ids 2,6,8): running avg [2.0, 4.0, 5.33..]
            var grp1Rows = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .sorted((a, b) -> Integer.compare(((Number) a.get(0)).intValue(), ((Number) b.get(0)).intValue()))
                    .toList();
            assertEquals(2.0, ((Number) grp1Rows.get(0).get(3)).doubleValue(), 0.01);
            assertEquals(4.0, ((Number) grp1Rows.get(1).get(3)).doubleValue(), 0.01);
            assertEquals(16.0 / 3, ((Number) grp1Rows.get(2).get(3)).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("running joinStrings — partition by grp, sort by id asc")
        void testRunningJoinStrings() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->ascending()), ~names:{p,w,r|$r.name}:y|$y->joinStrings(''))");
            assertEquals(10, r.rowCount());
            // Spot-check grp=1 (names B,F,H sorted by id 2,6,8): running ["B", "BF", "BFH"]
            var grp1Rows = r.rows().stream()
                    .filter(row -> ((Number) row.get(1)).intValue() == 1)
                    .sorted((a, b) -> Integer.compare(((Number) a.get(0)).intValue(), ((Number) b.get(0)).intValue()))
                    .toList();
            assertEquals("B", grp1Rows.get(0).get(3));
            assertEquals("BF", grp1Rows.get(1).get(3));
            assertEquals("BFH", grp1Rows.get(2).get(3));
        }
    }

    // ========================================================================
    // 12. Multiple partition columns
    // ========================================================================

    @Nested
    @DisplayName("Multiple partition columns")
    class MultiplePartitionColumns {

        @Test
        @DisplayName("over with two partition columns — aggregate")
        void testTwoPartitionColumnsAgg() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                        id, grp, grp2, name
                        1, 2, 2, A
                        2, 1, 1, B
                        3, 3, 3, C
                        4, 4, 4, D
                        5, 2, 2, E
                        6, 1, 2, F
                        7, 3, 3, G
                        8, 1, 1, H
                        9, 5, 5, I
                        10, 0, 0, J
                    #->extend(over(~[grp,grp2]), ~total:{p,w,r|$r.id}:y|$y->plus())""");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
        }

        @Test
        @DisplayName("over with two partition columns + sort — ranking")
        void testTwoPartitionColumnsRanking() throws SQLException {
            var r = executeRelation(
                    """
                            |#TDS
                                id, grp, grp2, name
                                1, 2, 2, A
                                2, 1, 1, B
                                3, 3, 3, C
                                4, 4, 4, D
                                5, 2, 2, E
                                6, 1, 2, F
                                7, 3, 3, G
                                8, 1, 1, H
                                9, 5, 5, I
                                10, 0, 0, J
                            #->extend(over(~[grp,grp2], ~id->descending()), ~[newCol:{p,w,r|$p->lead($r).id}, other:{p,w,r|$p->first($w,$r).name}])""");
            assertEquals(10, r.rowCount());
            assertEquals(6, r.columns().size());
        }

        @Test
        @DisplayName("over with two partition columns + sort — row number")
        void testTwoPartitionColumnsRowNumber() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                        id, grp, grp2
                        1, 1, 1
                        2, 1, 1
                        3, 1, 2
                        4, 2, 1
                    #->extend(over(~[grp,grp2], ~id->ascending()), ~rn:{p,w,r|$p->rowNumber($r)})""");
            assertEquals(4, r.rowCount());
            // (grp=1,grp2=1): ids 1,2 → rn 1,2; (grp=1,grp2=2): id 3 → rn 1;
            // (grp=2,grp2=1): id 4 → rn 1
            for (var row : r.rows()) {
                int id = ((Number) row.get(0)).intValue();
                int rn = ((Number) row.get(colIdx(r, "rn"))).intValue();
                int expected = switch (id) {
                    case 1 -> 1;
                    case 2 -> 2;
                    case 3 -> 1;
                    case 4 -> 1;
                    default -> -1;
                };
                assertEquals(expected, rn, "rn for id=" + id);
            }
        }
    }

    // ========================================================================
    // 13. Multiple window agg columns with mixed aggregation functions
    // ========================================================================

    @Nested
    @DisplayName("Mixed window aggregate columns")
    class MixedWindowAggColumns {

        @Test
        @DisplayName("sum and count together — partition only")
        void testSumAndCount() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~[total:{p,w,r|$r.id}:y|$y->plus(), cnt:{p,w,r|$r.id}:y|$y->size()])");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                int total = ((Number) row.get(3)).intValue();
                int cnt = ((Number) row.get(4)).intValue();
                if (grp == 1) {
                    assertEquals(16, total);
                    assertEquals(3, cnt);
                }
            }
        }

        @Test
        @DisplayName("min and max together — partition only")
        void testMinAndMax() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~[mn:{p,w,r|$r.id}:y|$y->min(), mx:{p,w,r|$r.id}:y|$y->max()])");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                if (grp == 1) {
                    assertEquals(2, ((Number) row.get(3)).intValue());
                    assertEquals(8, ((Number) row.get(4)).intValue());
                }
            }
        }

        @Test
        @DisplayName("sum and average together — partition+sort+frame")
        void testSumAndAvgWithFrame() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p, ~o->ascending(), unbounded()->rows(unbounded())), " +
                    "~[total:{p,w,r|$r.v}:y|$y->plus(), avg:{p,w,r|$r.v}:y|$y->average()])");
            assertEquals(5, r.rowCount());
            assertEquals(5, r.columns().size());
            // Unbounded frame: p=0 total=60, avg=20.0; p=1 total=300, avg=150.0
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                int total = ((Number) row.get(3)).intValue();
                double avg = ((Number) row.get(4)).doubleValue();
                if (p == 0) {
                    assertEquals(60, total);
                    assertEquals(20.0, avg, 0.01);
                } else {
                    assertEquals(300, total);
                    assertEquals(150.0, avg, 0.01);
                }
            }
        }

        @Test
        @DisplayName("joinStrings and plus together — partition+sort")
        void testJoinStringsAndPlus() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp, ~id->descending()), ~[names:{p,w,r|$r.name}:y|$y->joinStrings(''), total:{p,w,r|$r.id}:y|$y->plus()])");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
        }
    }

    // ========================================================================
    // 14. Multi-window pipeline — multiple window extends in sequence
    // ========================================================================

    @Nested
    @DisplayName("Multi-window pipelines")
    class MultiWindowPipelines {

        @Test
        @DisplayName("window agg → window ranking")
        void testAggThenRanking() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~total:{p,w,r|$r.id}:y|$y->plus())" +
                    "->extend(~grp->over(~total->descending()), ~rn:{p,w,r|$p->rowNumber($r)})");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
            // All rows in same partition have same total, so rn just orders by total desc
            for (var row : r.rows()) {
                long rn = ((Number) row.get(colIdx(r, "rn"))).longValue();
                assertTrue(rn >= 1, "rn should be >= 1");
            }
        }

        @Test
        @DisplayName("window ranking → no-window agg")
        void testRankingThenNoWindowAgg() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})" +
                    "->extend(~maxRn:c|$c.rn:y|$y->max())");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
            // maxRn should be 3 (grp=1 has 3 rows so max rn=3)
            for (var row : r.rows()) {
                assertEquals(3, ((Number) row.get(4)).intValue(), "maxRn should be 3");
            }
        }

        @Test
        @DisplayName("window agg → scalar extend → filter")
        void testWindowAggThenScalarThenFilter() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~partSum:{p,w,r|$r.id}:y|$y->plus())" +
                    "->extend(~pct: x | $x.id * 100 / $x.partSum)" +
                    "->filter(x | $x.pct > 40)");
            assertTrue(r.rowCount() > 0);
            assertEquals(5, r.columns().size());
        }

        @Test
        @DisplayName("two different window aggs in sequence")
        void testTwoWindowAggs() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~partSum:{p,w,r|$r.id}:y|$y->plus())" +
                    "->extend(over(~grp), ~partCnt:{p,w,r|$r.id}:y|$y->size())");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
            // Spot-check grp=1: partSum=16, partCnt=3
            for (var row : r.rows()) {
                int grp = ((Number) row.get(1)).intValue();
                if (grp == 1) {
                    assertEquals(16, ((Number) row.get(3)).intValue());
                    assertEquals(3, ((Number) row.get(4)).intValue());
                }
            }
        }

        @Test
        @DisplayName("window ranking → filter → groupBy")
        void testRankingFilterGroupBy() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->descending()), ~rn:{p,w,r|$p->rowNumber($r)})" +
                    "->filter(x | $x.rn == 1)" +
                    "->groupBy(~grp, ~topId:x|$x.id:y|$y->plus())");
            assertEquals(6, r.rowCount()); // 6 distinct groups
            // Each group's top row (rn=1, descending id) is the max id in that group
            for (var row : r.rows()) {
                int grp = ((Number) row.get(0)).intValue();
                int topId = ((Number) row.get(1)).intValue();
                int expected = switch (grp) {
                    case 0 -> 10;
                    case 1 -> 8;
                    case 2 -> 5;
                    case 3 -> 7;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> -1;
                };
                assertEquals(expected, topId, "topId for grp=" + grp);
            }
        }

        @Test
        @DisplayName("window lead → lag in sequence")
        void testLeadThenLag() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(~grp->over(~id->ascending()), ~next:{p,w,r|$p->lead($r).id})" +
                    "->extend(~grp->over(~id->ascending()), ~prev:{p,w,r|$p->lag($r).id})");
            assertEquals(10, r.rowCount());
            assertEquals(5, r.columns().size());
        }

        @Test
        @DisplayName("window extend → sort → slice")
        void testWindowSortSlice() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~partSum:{p,w,r|$r.id}:y|$y->plus())" +
                    "->sort(descending(~partSum))" +
                    "->slice(0, 5)");
            assertEquals(5, r.rowCount());
            // After sort desc by partSum, first row should have highest partSum
            int first = ((Number) r.rows().get(0).get(colIdx(r, "partSum"))).intValue();
            int last = ((Number) r.rows().get(4).get(colIdx(r, "partSum"))).intValue();
            assertTrue(first >= last, "Results should be sorted descending by partSum");
        }

        @Test
        @DisplayName("window extend → select projection")
        void testWindowThenSelect() throws SQLException {
            var r = executeRelation(TDS10 +
                    "->extend(over(~grp), ~total:{p,w,r|$r.id}:y|$y->plus())" +
                    "->select(~[grp, total])");
            assertEquals(10, r.rowCount());
            assertEquals(2, r.columns().size());
        }
    }

    // ========================================================================
    // 14. Statistical aggregate functions — mean, median, mode, stdDev*, variance*,
    // percentile*
    // ========================================================================

    @Nested
    @DisplayName("Statistical aggregates in extend")
    class StatisticalAggregates {

        @Test
        @DisplayName("mean — alias for average (no-window)")
        void testMean() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~m:c|$c.v:y|$y->mean())");
            assertEquals(5, r.rowCount());
            // mean of all values: (10+20+30+100+200)/5 = 72.0
            assertEquals(72.0, ((Number) r.rows().get(0).get(colIdx(r, "m"))).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("mean — windowed with partition")
        void testMeanWindowed() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p), ~m:{p,w,r|$r.v}:y|$y->mean())");
            assertEquals(5, r.rowCount());
            // p=0: mean(10,20,30)=20.0, p=1: mean(100,200)=150.0
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                double m = ((Number) row.get(3)).doubleValue();
                if (p == 0)
                    assertEquals(20.0, m, 0.01);
                else
                    assertEquals(150.0, m, 0.01);
            }
        }

        @Test
        @DisplayName("median — no-window")
        void testMedian() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~med:c|$c.v:y|$y->median())");
            assertEquals(5, r.rowCount());
            // Median of [10,20,30,100,200] = 30 (middle value)
            double med = ((Number) r.rows().get(0).get(colIdx(r, "med"))).doubleValue();
            assertEquals(30.0, med, 0.01);
        }

        @Test
        @DisplayName("median — windowed with partition")
        void testMedianWindowed() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p), ~med:{p,w,r|$r.v}:y|$y->median())");
            assertEquals(5, r.rowCount());
            // p=0: median(10,20,30)=20, p=1: median(100,200)=150
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                double med = ((Number) row.get(3)).doubleValue();
                if (p == 0)
                    assertEquals(20.0, med, 0.01);
                else
                    assertEquals(150.0, med, 0.01);
            }
        }

        @Test
        @DisplayName("mode — no-window")
        void testMode() throws SQLException {
            var r = executeRelation("""
                    |#TDS
                        id, val
                        1, 10
                        2, 20
                        3, 10
                        4, 30
                        5, 10
                    #->extend(~md:c|$c.val:y|$y->mode())""");
            assertEquals(5, r.rowCount());
            // Mode of [10,20,10,30,10] = 10
            assertEquals(10, ((Number) r.rows().get(0).get(colIdx(r, "md"))).intValue());
        }

        @Test
        @DisplayName("stdDevSample — no-window")
        void testStdDevSample() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~sd:c|$c.v:y|$y->stdDevSample())");
            assertEquals(5, r.rowCount());
            double sd = ((Number) r.rows().get(0).get(colIdx(r, "sd"))).doubleValue();
            // stddev_samp([10,20,30,100,200]) ≈ 79.81 (mean=72, sum_sq_dev=25480, /4=6370)
            assertEquals(79.81, sd, 0.1);
        }

        @Test
        @DisplayName("stdDevPopulation — no-window")
        void testStdDevPopulation() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~sd:c|$c.v:y|$y->stdDevPopulation())");
            assertEquals(5, r.rowCount());
            double sd = ((Number) r.rows().get(0).get(colIdx(r, "sd"))).doubleValue();
            // stddev_pop([10,20,30,100,200]) ≈ 71.39 (mean=72, sum_sq_dev=25480, /5=5096)
            assertEquals(71.39, sd, 0.1);
        }

        @Test
        @DisplayName("varianceSample — no-window")
        void testVarianceSample() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~vr:c|$c.v:y|$y->varianceSample())");
            assertEquals(5, r.rowCount());
            double vr = ((Number) r.rows().get(0).get(colIdx(r, "vr"))).doubleValue();
            // var_samp([10,20,30,100,200]) = 6370.0 (sum_sq_dev=25480 / (5-1))
            assertEquals(6370.0, vr, 1.0);
        }

        @Test
        @DisplayName("variancePopulation — no-window")
        void testVariancePopulation() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~vr:c|$c.v:y|$y->variancePopulation())");
            assertEquals(5, r.rowCount());
            double vr = ((Number) r.rows().get(0).get(colIdx(r, "vr"))).doubleValue();
            // var_pop([10,20,30,100,200]) = 5096.0 (sum_sq_dev=25480 / 5)
            assertEquals(5096.0, vr, 1.0);
        }

        @Test
        @DisplayName("percentile — 50th percentile (no-window)")
        void testPercentile() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~p50:c|$c.v:y|$y->percentile(0.5))");
            assertEquals(5, r.rowCount());
            double p50 = ((Number) r.rows().get(0).get(colIdx(r, "p50"))).doubleValue();
            // 50th percentile of [10,20,30,100,200] = 30
            assertEquals(30.0, p50, 0.01);
        }

        @Test
        @DisplayName("percentileCont — continuous 50th percentile (no-window)")
        void testPercentileCont() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~pc:c|$c.v:y|$y->percentileCont(0.5))");
            assertEquals(5, r.rowCount());
            double pc = ((Number) r.rows().get(0).get(colIdx(r, "pc"))).doubleValue();
            // percentile_cont(0.5) on [10,20,30,100,200] = 30
            assertEquals(30.0, pc, 0.01);
        }

        @Test
        @DisplayName("percentileDisc — discrete 50th percentile (no-window)")
        void testPercentileDisc() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(~pd:c|$c.v:y|$y->percentileDisc(0.5))");
            assertEquals(5, r.rowCount());
            double pd = ((Number) r.rows().get(0).get(colIdx(r, "pd"))).doubleValue();
            // percentile_disc(0.5) on [10,20,30,100,200] = 30
            assertEquals(30.0, pd, 0.01);
        }

        @Test
        @DisplayName("stdDevSample — windowed with partition")
        void testStdDevSampleWindowed() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p), ~sd:{p,w,r|$r.v}:y|$y->stdDevSample())");
            assertEquals(5, r.rowCount());
            // p=0: stddev_samp(10,20,30)=10.0, p=1: stddev_samp(100,200)≈70.71
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                double sd = ((Number) row.get(3)).doubleValue();
                if (p == 0)
                    assertEquals(10.0, sd, 0.01);
                else
                    assertEquals(70.71, sd, 0.5);
            }
        }

        @Test
        @DisplayName("variancePopulation — windowed with partition")
        void testVariancePopulationWindowed() throws SQLException {
            var r = executeRelation(NUMS +
                    "->extend(over(~p), ~vr:{p,w,r|$r.v}:y|$y->variancePopulation())");
            assertEquals(5, r.rowCount());
            // p=0: var_pop(10,20,30)=66.67, p=1: var_pop(100,200)=2500.0
            for (var row : r.rows()) {
                int p = ((Number) row.get(0)).intValue();
                double vr = ((Number) row.get(3)).doubleValue();
                if (p == 0)
                    assertEquals(66.67, vr, 0.5);
                else
                    assertEquals(2500.0, vr, 0.01);
            }
        }
    }
}
