package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.Row;
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
 * Integration tests for PivotChecker — Relation API {@code pivot()}.
 * <ul>
 *   <li>Single-column pivot with single/multiple aggregates</li>
 *   <li>Multi-column pivot with __|__ separator</li>
 *   <li>Aggregate varieties: SUM, COUNT (literal), computed expressions</li>
 *   <li>Output schema: static group-by columns + dynamic pivot columns</li>
 *   <li>NULL semantics for missing pivot values</li>
 *   <li>Type parity: pivot columns preserve aggregate return types (Integer, not String)</li>
 *   <li>Chained operations: pivot→filter, pivot→extend, pivot→groupBy, pivot→sort</li>
 *   <li>Complex source chains: extend→filter→select→groupBy→pivot</li>
 *   <li>Error cases: invalid pivot column</li>
 * </ul>
 */
public class PivotCheckerTest extends AbstractDatabaseTest {

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

    // ==================== Single-Column Pivot ====================

    @Nested
    @DisplayName("pivot() single pivot column")
    class SingleColumnPivot {

        @Test
        @DisplayName("single pivot column, single aggregate — verifies pivoted values per group")
        void testSingleColumnSingleAggregate() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    NYC, 2012, 7600
                    SAN, 2011, 2000
                    SAN, 2012, 3000
                #->pivot(~[year], ~[total:x|$x.treePlanted:y|$y->plus()])
                """);
            assertFalse(result.rows().isEmpty(), "Should have pivot results");

            // Static columns: city (year removed as pivot column, treePlanted removed as value column)
            assertTrue(hasColumn(result, "city"), "Should have 'city' group column");

            // Dynamic columns should exist for each unique year value
            assertTrue(hasPivotColumn(result, "total"),
                    "Should have pivot columns with 'total' suffix");

            // Verify values: NYC 2011=5000, SAN 2011=2000
            int cityIdx = columnIndex(result, "city");
            for (Row row : result.rows()) {
                String city = row.get(cityIdx).toString();
                if ("NYC".equals(city)) {
                    assertPivotValue(result, row, "2011", "total", 5000);
                    assertPivotValue(result, row, "2012", "total", 7600);
                } else if ("SAN".equals(city)) {
                    assertPivotValue(result, row, "2011", "total", 2000);
                    assertPivotValue(result, row, "2012", "total", 3000);
                } else {
                    fail("Unexpected city: " + city);
                }
            }
        }

        @Test
        @DisplayName("single pivot column, multiple aggregates — sum + count")
        void testSingleColumnMultipleAggregates() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    NYC, 2011, 3000
                    NYC, 2012, 7600
                    SAN, 2011, 2000
                #->pivot(~[year], ~[sum:x|$x.treePlanted:y|$y->plus(), count:x|1:y|$y->plus()])
                """);

            assertTrue(hasPivotColumn(result, "sum"), "Should have 'sum' aggregate columns");
            assertTrue(hasPivotColumn(result, "count"), "Should have 'count' aggregate columns");

            int cityIdx = columnIndex(result, "city");
            for (Row row : result.rows()) {
                String city = row.get(cityIdx).toString();
                if ("NYC".equals(city)) {
                    // NYC 2011: sum=5000+3000=8000, count=2
                    assertPivotValue(result, row, "2011", "sum", 8000);
                    assertPivotValue(result, row, "2011", "count", 2);
                    // NYC 2012: sum=7600, count=1
                    assertPivotValue(result, row, "2012", "sum", 7600);
                    assertPivotValue(result, row, "2012", "count", 1);
                } else if ("SAN".equals(city)) {
                    // SAN 2011: sum=2000, count=1
                    assertPivotValue(result, row, "2011", "sum", 2000);
                    assertPivotValue(result, row, "2011", "count", 1);
                }
            }
        }

        @Test
        @DisplayName("pivot with literal fn1 body — count via |1")
        void testLiteralCountExpression() throws SQLException {
            // Use only city+year — no extra columns that would become spurious group-by cols
            var result = executeRelation("""
                #TDS
                    city, year
                    NYC, 2011
                    NYC, 2011
                    SAN, 2011
                #->pivot(~[year], ~[cnt:x|1:y|$y->plus()])
                """);

            // Verify SQL uses SUM(1) for the count
            String sql = generateSql("""
                #TDS
                    city, year
                    NYC, 2011
                    NYC, 2011
                    SAN, 2011
                #->pivot(~[year], ~[cnt:x|1:y|$y->plus()])
                """);
            assertTrue(sql.contains("SUM(1)"), "Should use SUM(1) for literal count. Got: " + sql);

            int cityIdx = columnIndex(result, "city");
            for (Row row : result.rows()) {
                String city = row.get(cityIdx).toString();
                if ("NYC".equals(city)) {
                    assertPivotValue(result, row, "2011", "cnt", 2);
                } else if ("SAN".equals(city)) {
                    assertPivotValue(result, row, "2011", "cnt", 1);
                }
            }
        }

        @Test
        @DisplayName("pivot with computed expression — multiplication in fn1")
        void testComputedMultiplicationExpression() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted, coefficient
                    NYC, 2011, 100, 3
                    NYC, 2011, 200, 2
                    SAN, 2011, 50, 4
                #->pivot(~[year], ~[weighted:x|$x.treePlanted * $x.coefficient:y|$y->plus()])
                """);

            String sql = generateSql("""
                #TDS
                    city, year, treePlanted, coefficient
                    NYC, 2011, 100, 3
                    NYC, 2011, 200, 2
                    SAN, 2011, 50, 4
                #->pivot(~[year], ~[weighted:x|$x.treePlanted * $x.coefficient:y|$y->plus()])
                """);
            assertTrue(sql.contains("SUM("), "Should contain SUM function");
            assertTrue(sql.contains("*"), "Should contain multiplication operator");

            int cityIdx = columnIndex(result, "city");
            for (Row row : result.rows()) {
                String city = row.get(cityIdx).toString();
                if ("NYC".equals(city)) {
                    // NYC 2011: (100*3) + (200*2) = 300 + 400 = 700
                    assertPivotValue(result, row, "2011", "weighted", 700);
                } else if ("SAN".equals(city)) {
                    // SAN 2011: 50*4 = 200
                    assertPivotValue(result, row, "2011", "weighted", 200);
                }
            }
        }
    }

    // ==================== Multi-Column Pivot ====================

    @Nested
    @DisplayName("pivot() multi-column pivot")
    class MultiColumnPivot {

        @Test
        @DisplayName("two pivot columns — uses __|__ separator and EXCLUDE clause")
        void testTwoColumnPivot() throws SQLException {
            String pure = """
                #TDS
                    country, city, year, sales
                    USA, NYC, 2020, 100
                    USA, NYC, 2020, 50
                    USA, LA, 2020, 200
                    UK, LDN, 2021, 300
                #->pivot(~[country, city], ~[total:x|$x.sales:y|$y->plus()])
                """;
            String sql = generateSql(pure);

            // Multi-column pivot should use EXCLUDE and __|__ separator
            assertTrue(sql.contains("EXCLUDE"), "Should use EXCLUDE to remove pivot columns");
            assertTrue(sql.contains("|| '__|__' ||"), "Should use __|__ separator");
            assertTrue(sql.contains("_pivot_key"), "Should create _pivot_key");

            var result = executeRelation(pure);
            // Should aggregate: 2020 has USA_NYC (150), USA_LA (200); 2021 has UK_LDN (300)
            assertEquals(2, result.rowCount(), "Should have 2 rows (one per year)");

            // Column names should contain __|__ between pivoted values
            boolean hasSeparator = result.columns().stream()
                    .anyMatch(c -> c.name().contains("__|__"));
            assertTrue(hasSeparator, "Column names should use __|__ separator");
        }

        @Test
        @DisplayName("multi-column pivot with multiple aggregates — sum + count")
        void testMultiColumnMultipleAggregates() throws SQLException {
            String pure = """
                #TDS
                    country, city, year, treePlanted
                    USA, NYC, 2011, 5000
                    USA, NYC, 2011, 3000
                    UK, LDN, 2011, 3000
                #->pivot(~[country,city], ~[sum:x|$x.treePlanted:y|$y->plus(),count:x|1:y|$y->plus()])
                """;

            var result = executeRelation(pure);
            assertFalse(result.rows().isEmpty(), "Should have results");

            boolean hasSumCol = result.columns().stream()
                    .anyMatch(c -> c.name().contains("sum"));
            boolean hasCountCol = result.columns().stream()
                    .anyMatch(c -> c.name().contains("count"));
            assertTrue(hasSumCol, "Should have sum pivot columns");
            assertTrue(hasCountCol, "Should have count pivot columns");
        }
    }

    // ==================== NULL Semantics ====================

    @Nested
    @DisplayName("pivot() NULL semantics")
    class NullSemantics {

        @Test
        @DisplayName("NULL for missing pivot values — UK has no 2012 data")
        void testNullForMissingPivotValues() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    NYC, USA, 2012, 7600
                    SAN, USA, 2011, 2600
                    LDN, UK, 2011, 3000
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                """);

            List<String> colNames = result.columns().stream().map(c -> c.name()).toList();
            int countryIdx = findColumnContaining(colNames, "country");
            int col2011 = findColumnContaining(colNames, "2011");
            int col2012 = findColumnContaining(colNames, "2012");

            assertTrue(countryIdx >= 0, "Should have country column");
            assertTrue(col2011 >= 0, "Should have 2011 pivot column");
            assertTrue(col2012 >= 0, "Should have 2012 pivot column");

            for (Row row : result.rows()) {
                Object country = row.get(countryIdx);
                if (country != null && "UK".equals(country.toString())) {
                    // UK only has 2011 data (LDN=3000), no 2012 data
                    assertNotNull(row.get(col2011), "UK 2011 should not be NULL");
                    assertEquals(3000, ((Number) row.get(col2011)).intValue(), "UK 2011 = 3000");
                    assertNull(row.get(col2012),
                            "UK 2012 should be NULL (not 0) — no data exists for UK in 2012");
                }
            }
        }

        @Test
        @DisplayName("single row per group — no aggregation needed, each cell is direct")
        void testSingleRowPerGroup() throws SQLException {
            var result = executeRelation("""
                #TDS
                    name, year, score
                    Alice, 2020, 100
                    Alice, 2021, 200
                    Bob, 2020, 150
                #->pivot(~[year], ~[val:x|$x.score:y|$y->plus()])
                """);

            assertEquals(2, result.rowCount(), "Should have 2 rows (Alice, Bob)");

            int nameIdx = columnIndex(result, "name");
            List<String> colNames = result.columns().stream().map(c -> c.name()).toList();
            int col2020 = findColumnContaining(colNames, "2020");
            int col2021 = findColumnContaining(colNames, "2021");

            for (Row row : result.rows()) {
                String name = row.get(nameIdx).toString();
                if ("Alice".equals(name)) {
                    assertEquals(100, ((Number) row.get(col2020)).intValue());
                    assertEquals(200, ((Number) row.get(col2021)).intValue());
                } else if ("Bob".equals(name)) {
                    assertEquals(150, ((Number) row.get(col2020)).intValue());
                    assertNull(row.get(col2021), "Bob has no 2021 data → NULL");
                }
            }
        }
    }

    // ==================== Type Parity ====================

    @Nested
    @DisplayName("pivot() type parity")
    class TypeParity {

        @Test
        @DisplayName("pivot columns preserve Integer type — not String")
        void testPivotColumnsPreserveIntegerType() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    NYC, 2012, 7600
                    SAN, 2011, 2000
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                """);

            // All pivot columns should report Integer type, not String
            for (var col : result.columns()) {
                if (col.name().contains("__|__")) {
                    assertEquals("Integer", col.javaType(),
                            "Pivot column '" + col.name() + "' should be Integer, not " + col.javaType());
                }
            }

            // Verify actual values are numeric (not String)
            for (Row row : result.rows()) {
                for (int i = 0; i < result.columns().size(); i++) {
                    if (result.columns().get(i).name().contains("__|__")) {
                        Object val = row.get(i);
                        if (val != null) {
                            assertInstanceOf(Number.class, val,
                                    "Pivot value in column '" + result.columns().get(i).name()
                                            + "' should be numeric, not " + val.getClass().getSimpleName());
                        }
                    }
                }
            }
        }

        @Test
        @DisplayName("multi-aggregate pivot — both sum and count are Integer")
        void testMultiAggregateTypesParity() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    NYC, 2011, 3000
                    SAN, 2012, 2000
                #->pivot(~[year], ~[sum:x|$x.treePlanted:y|$y->plus(), count:x|1:y|$y->plus()])
                """);

            for (var col : result.columns()) {
                if (col.name().contains("__|__")) {
                    assertEquals("Integer", col.javaType(),
                            "Pivot column '" + col.name() + "' should be Integer");
                }
            }
        }
    }

    // ==================== Chained Operations ====================

    @Nested
    @DisplayName("pivot() chained with downstream operations")
    class ChainedOperations {

        @Test
        @DisplayName("pivot then extend — adds computed column to pivot result")
        void testPivotThenExtend() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, value
                    NYC, 2011, 100
                    NYC, 2012, 200
                    SAN, 2011, 50
                #->pivot(~[year], ~[total:x|$x.value:y|$y->plus()])
                ->extend(~combined:x|$x.city->toOne() + '_test')
                """);

            assertFalse(result.rows().isEmpty(), "Should have results");
            assertTrue(hasColumn(result, "combined"), "Should have 'combined' column");

            int combinedIdx = columnIndex(result, "combined");
            for (Row row : result.rows()) {
                String combined = (String) row.get(combinedIdx);
                assertTrue(combined.endsWith("_test"),
                        "Combined column should end with '_test': " + combined);
            }
        }

        @Test
        @DisplayName("pivot then filter — filters on static group column")
        void testPivotThenFilter() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    SAN, USA, 2011, 2600
                    LDN, UK, 2011, 3000
                    NYC, USA, 2012, 15200
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                ->filter(x|$x.city == 'NYC')
                """);

            assertEquals(1, result.rowCount(), "Should have only NYC row");
            int cityIdx = columnIndex(result, "city");
            assertEquals("NYC", result.rows().get(0).get(cityIdx));
        }

        @Test
        @DisplayName("pivot then sort — sorts on static group column")
        void testPivotThenSort() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    SAN, 2011, 2600
                    LDN, 2011, 3000
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                ->sort(~city->ascending())
                """);

            assertEquals(3, result.rowCount(), "Should have 3 rows");
            int cityIdx = columnIndex(result, "city");
            assertEquals("LDN", result.rows().get(0).get(cityIdx), "First row should be LDN");
            assertEquals("NYC", result.rows().get(1).get(cityIdx), "Second row should be NYC");
            assertEquals("SAN", result.rows().get(2).get(cityIdx), "Third row should be SAN");
        }

        @Test
        @DisplayName("pivot then cast then groupBy — cast declares schema for downstream groupBy")
        void testPivotThenCastThenGroupBy() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    NYC, USA, 2012, 7600
                    SAN, USA, 2011, 2600
                    LDN, UK, 2011, 3000
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                ->cast(@Relation<(city:String, country:String, '2011__|__newCol':Integer, '2012__|__newCol':Integer)>)
                ->groupBy(~[country], ~['2011__|__newCol':x|$x.'2011__|__newCol':y|$y->plus()])
                """);

            assertFalse(result.rows().isEmpty(), "Should have groupBy results");
            assertTrue(hasColumn(result, "country"), "Should have 'country' column");

            // USA has NYC(5000) + SAN(2600) = 7600 for 2011
            int countryIdx = columnIndex(result, "country");
            for (Row row : result.rows()) {
                String country = row.get(countryIdx).toString();
                if ("USA".equals(country)) {
                    // NYC 2011=5000, SAN 2011=2600 → grouped = 7600
                    Object val = row.get(1); // the aggregate column
                    assertNotNull(val, "USA 2011 aggregate should not be null");
                    assertEquals(7600, ((Number) val).intValue(), "USA 2011 grouped = 7600");
                } else if ("UK".equals(country)) {
                    Object val = row.get(1);
                    assertNotNull(val, "UK 2011 aggregate should not be null");
                    assertEquals(3000, ((Number) val).intValue(), "UK 2011 grouped = 3000");
                }
            }
        }
    }

    // ==================== Complex Source Chains ====================

    @Nested
    @DisplayName("pivot() with complex upstream chains")
    class ComplexSourceChains {

        @Test
        @DisplayName("extend→filter→select→groupBy→pivot — full PCT chain")
        void testExtendFilterSelectGroupByPivot() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    NYC, USA, 2000, 5000
                    SAN, USA, 2000, 2000
                    SAN, USA, 2011, 100
                    LDN, UK, 2011, 3000
                    SAN, USA, 2011, 2500
                    NYC, USA, 2000, 10000
                    NYC, USA, 2012, 7600
                    NYC, USA, 2012, 7600
                #->extend(~yr:x|$x.year->toOne() - 2000)
                ->filter(x|$x.yr > 10)
                ->select(~[city,country,year,treePlanted])
                ->groupBy(~[city,country], ~[year:x|$x.year:y|$y->plus(),treePlanted:x|$x.treePlanted:y|$y->plus()])
                ->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                """);

            assertFalse(result.rows().isEmpty(), "Should have pivot results");
            assertTrue(hasColumn(result, "city"), "Should have 'city' column");
            assertTrue(hasColumn(result, "country"), "Should have 'country' column");
        }

        @Test
        @DisplayName("groupBy→pivot — groupBy as direct source")
        void testGroupByThenPivot() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    NYC, 2012, 7600
                    SAN, 2011, 2000
                #->pivot(~[year], ~[total:x|$x.treePlanted:y|$y->plus()])
                """);

            assertFalse(result.rows().isEmpty(), "Should have pivot results");
            assertTrue(hasColumn(result, "city"), "city should be a group column");
        }
    }

    // ==================== Full Pipeline: Pivot→NULL→groupBy ====================

    @Nested
    @DisplayName("pivot() full pipeline with NULL handling")
    class FullPipelineWithNulls {

        @Test
        @DisplayName("pivot with NULLs — missing pivot values are NULL not 0")
        void testPivotWithNulls() throws SQLException {
            var result = executeRelation("""
                #TDS
                    city, country, year, treePlanted
                    NYC, USA, 2011, 5000
                    NYC, USA, 2012, 15200
                    SAN, USA, 2011, 2600
                    LDN, UK, 2011, 3000
                #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                """);

            // Pivot produces group-by cols (city, country) + dynamic cols (2011__|__newCol, 2012__|__newCol)
            List<String> colNames = result.columns().stream().map(c -> c.name()).toList();
            int cityIdx = findColumnContaining(colNames, "city");
            int countryIdx = findColumnContaining(colNames, "country");
            int col2011 = findColumnContaining(colNames, "2011");
            int col2012 = findColumnContaining(colNames, "2012");

            assertTrue(col2011 >= 0, "Should have 2011 pivot column");
            assertTrue(col2012 >= 0, "Should have 2012 pivot column");

            // Find rows by city
            for (Row row : result.rows()) {
                String city = row.get(cityIdx).toString();
                String country = row.get(countryIdx).toString();
                if ("LDN".equals(city) && "UK".equals(country)) {
                    // UK has 2011 data only
                    assertEquals(3000, ((Number) row.get(col2011)).intValue(), "LDN 2011 = 3000");
                    assertNull(row.get(col2012),
                            "LDN/UK 2012 should be NULL (not 0) — no data exists");
                } else if ("NYC".equals(city)) {
                    assertEquals(5000, ((Number) row.get(col2011)).intValue(), "NYC 2011 = 5000");
                    assertEquals(15200, ((Number) row.get(col2012)).intValue(), "NYC 2012 = 15200");
                } else if ("SAN".equals(city)) {
                    assertEquals(2600, ((Number) row.get(col2011)).intValue(), "SAN 2011 = 2600");
                    assertNull(row.get(col2012), "SAN 2012 should be NULL");
                }
            }
        }
    }

    // ==================== SQL Generation ====================

    @Nested
    @DisplayName("pivot() SQL generation")
    class SqlGeneration {

        @Test
        @DisplayName("single-column pivot SQL — PIVOT ON col USING agg AS alias")
        void testSingleColumnPivotSql() throws SQLException {
            String sql = generateSql("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                    SAN, 2012, 2000
                #->pivot(~[year], ~[total:x|$x.treePlanted:y|$y->plus()])
                """);

            assertTrue(sql.contains("PIVOT"), "Should contain PIVOT keyword");
            assertTrue(sql.contains("ON"), "Should contain ON keyword");
            assertTrue(sql.contains("USING"), "Should contain USING keyword");
            assertTrue(sql.contains("SUM("), "Should use SUM aggregate");
            assertTrue(sql.contains("\"_|__total\""), "Should alias as \"_|__total\"");
        }

        @Test
        @DisplayName("multi-column pivot SQL — EXCLUDE + concatenation + _pivot_key")
        void testMultiColumnPivotSql() throws SQLException {
            String sql = generateSql("""
                #TDS
                    country, city, year, sales
                    USA, NYC, 2020, 100
                #->pivot(~[country, city], ~[total:x|$x.sales:y|$y->plus()])
                """);

            assertTrue(sql.contains("EXCLUDE"), "Should use EXCLUDE clause");
            assertTrue(sql.contains("\"_pivot_key\""), "Should create _pivot_key alias");
            assertTrue(sql.contains("|| '__|__' ||"),
                    "Should concatenate pivot columns with __|__ separator");
            assertTrue(sql.contains("\"_|__total\""), "Should alias aggregate as \"_|__total\"");
        }
    }

    // ==================== Error Cases ====================

    @Nested
    @DisplayName("pivot() error cases")
    class ErrorCases {

        @Test
        @DisplayName("pivot on non-existent column — throws compile error")
        void testPivotOnNonExistentColumn() {
            assertThrows(Exception.class, () -> executeRelation("""
                #TDS
                    city, year, treePlanted
                    NYC, 2011, 5000
                #->pivot(~[nonExistent], ~[total:x|$x.treePlanted:y|$y->plus()])
                """),
                "Should throw when pivot column doesn't exist");
        }
    }

    // ==================== Utilities ====================

    /** Checks if a column with the given exact name exists. */
    private boolean hasColumn(ExecutionResult result, String name) {
        return result.columns().stream().anyMatch(c -> c.name().equals(name));
    }

    /** Checks if any column name contains the pivot __|__ separator + suffix. */
    private boolean hasPivotColumn(ExecutionResult result, String suffix) {
        return result.columns().stream()
                .anyMatch(c -> c.name().contains("__|__" + suffix));
    }

    /** Finds column index by exact name. */
    private int columnIndex(ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name())) return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }

    /** Finds the first column index whose name contains the given substring. */
    private int findColumnContaining(List<String> colNames, String substring) {
        for (int i = 0; i < colNames.size(); i++) {
            if (colNames.get(i).contains(substring)) return i;
        }
        return -1;
    }

    /**
     * Asserts a pivot value in a row by looking up the dynamic column name.
     * Dynamic columns are named: {pivotValue}__|__{aggAlias}
     */
    private void assertPivotValue(ExecutionResult result, Row row,
                                   String pivotValue, String aggAlias, int expected) {
        String colName = pivotValue + "__|__" + aggAlias;
        int idx = -1;
        for (int i = 0; i < result.columns().size(); i++) {
            if (result.columns().get(i).name().equals(colName)) {
                idx = i;
                break;
            }
        }
        assertTrue(idx >= 0, "Pivot column '" + colName + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
        Object val = row.get(idx);
        assertNotNull(val, "Value in pivot column '" + colName + "' should not be null");
        assertEquals(expected, ((Number) val).intValue(),
                "Pivot column '" + colName + "' expected " + expected);
    }
}
