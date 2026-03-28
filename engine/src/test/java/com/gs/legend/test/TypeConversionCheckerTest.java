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
 * Integration tests for cast() — validates that cast enables meaningful
 * downstream operations that require the target type.
 *
 * <p>
 * Philosophy: every test should DO SOMETHING with the casted value that
 * would fail without the cast, and assert exact output values — not just
 * row counts or column existence.
 */
public class TypeConversionCheckerTest extends AbstractDatabaseTest {

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

    // ==================== String → Integer cast + numeric downstream
    // ====================

    @Nested
    @DisplayName("cast String→Integer: enable numeric operations")
    class StringToInteger {

        @Test
        @DisplayName("cast column to Integer, then sum — proves cast enables aggregation")
        void testCastThenSum() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        product, amount:String
                        Widget, 100
                        Gadget, 250
                        Widget, 150
                    #->groupBy(~[product], ~[total:x|$x.amount->toOne()->cast(@Integer):y|$y->plus()])
                    ->sort(~product->ascending())
                    """);
            int prodIdx = columnIndex(result, "product");
            int totalIdx = columnIndex(result, "total");

            assertEquals(2, result.rows().size(), "Two groups: Gadget, Widget");
            assertEquals("Gadget", result.rows().get(0).get(prodIdx));
            assertEquals(250, ((Number) result.rows().get(0).get(totalIdx)).intValue(),
                    "Gadget: single value 250");
            assertEquals("Widget", result.rows().get(1).get(prodIdx));
            assertEquals(250, ((Number) result.rows().get(1).get(totalIdx)).intValue(),
                    "Widget: 100 + 150 = 250");
        }

        @Test
        @DisplayName("cast to Integer, then filter on numeric comparison — verify surviving rows")
        void testCastThenFilterNumeric() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        item, price:String
                        A, 50
                        B, 200
                        C, 75
                    #->filter(x|$x.price->toOne()->cast(@Integer) > 60)
                    ->sort(~item->ascending())
                    """);
            int itemIdx = columnIndex(result, "item");
            int priceIdx = columnIndex(result, "price");

            assertEquals(2, result.rows().size(), "B(200) and C(75) survive, A(50) dropped");
            assertEquals("B", result.rows().get(0).get(itemIdx));
            assertEquals("200", result.rows().get(0).get(priceIdx).toString());
            assertEquals("C", result.rows().get(1).get(itemIdx));
            assertEquals("75", result.rows().get(1).get(priceIdx).toString());
        }

        @Test
        @DisplayName("cast to Integer, then extend with arithmetic — verify computed values")
        void testCastThenArithmetic() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        item, qty:String, unitPrice:String
                        A, 3, 10
                        B, 5, 20
                    #->extend(~lineTotal:x|$x.qty->toOne()->cast(@Integer) * $x.unitPrice->toOne()->cast(@Integer))
                    ->sort(~item->ascending())
                    """);
            int itemIdx = columnIndex(result, "item");
            int totalIdx = columnIndex(result, "lineTotal");

            assertEquals(2, result.rows().size());
            assertEquals("A", result.rows().get(0).get(itemIdx));
            assertEquals(30, ((Number) result.rows().get(0).get(totalIdx)).intValue(), "A: 3×10=30");
            assertEquals("B", result.rows().get(1).get(itemIdx));
            assertEquals(100, ((Number) result.rows().get(1).get(totalIdx)).intValue(), "B: 5×20=100");
        }
    }

    // ==================== Pivot → Cast → downstream operations
    // ====================

    @Nested
    @DisplayName("pivot → cast: unlock dynamic pivot columns for downstream use")
    class PivotCast {

        @Test
        @DisplayName("pivot → cast → extend sums pivot columns — verify computed total")
        void testPivotCastExtendSum() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        city, year, trees
                        NYC, 2011, 5000
                        NYC, 2012, 7600
                        SAN, 2011, 2000
                        SAN, 2012, 3000
                    #->pivot(~[year], ~[total:x|$x.trees:y|$y->plus()])
                    ->cast(@Relation<(city:String, '2011__|__total':Integer, '2012__|__total':Integer)>)
                    ->extend(~allYears:x|$x.'2011__|__total'->toOne() + $x.'2012__|__total'->toOne())
                    ->sort(~city->ascending())
                    """);
            int cityIdx = columnIndex(result, "city");
            int y2011Idx = columnIndex(result, "2011__|__total");
            int y2012Idx = columnIndex(result, "2012__|__total");
            int allIdx = columnIndex(result, "allYears");

            assertEquals(2, result.rows().size());

            // NYC row
            assertEquals("NYC", result.rows().get(0).get(cityIdx));
            assertEquals(5000, ((Number) result.rows().get(0).get(y2011Idx)).intValue());
            assertEquals(7600, ((Number) result.rows().get(0).get(y2012Idx)).intValue());
            assertEquals(12600, ((Number) result.rows().get(0).get(allIdx)).intValue(),
                    "NYC allYears: 5000+7600=12600");

            // SAN row
            assertEquals("SAN", result.rows().get(1).get(cityIdx));
            assertEquals(2000, ((Number) result.rows().get(1).get(y2011Idx)).intValue());
            assertEquals(3000, ((Number) result.rows().get(1).get(y2012Idx)).intValue());
            assertEquals(5000, ((Number) result.rows().get(1).get(allIdx)).intValue(),
                    "SAN allYears: 2000+3000=5000");
        }

        @Test
        @DisplayName("pivot → cast → groupBy aggregates pivot columns by country")
        void testPivotCastGroupBy() throws SQLException {
            var result = executeRelation(
                    """
                            #TDS
                                city, country, year, treePlanted
                                NYC, USA, 2011, 5000
                                NYC, USA, 2012, 7600
                                SAN, USA, 2011, 2600
                                LDN, UK, 2011, 3000
                            #->pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
                            ->cast(@Relation<(city:String, country:String, '2011__|__newCol':Integer, '2012__|__newCol':Integer)>)
                            ->groupBy(~[country], ~['2011__|__newCol':x|$x.'2011__|__newCol':y|$y->plus()])
                            ->sort(~country->ascending())
                            """);
            int countryIdx = columnIndex(result, "country");
            int aggIdx = columnIndex(result, "2011__|__newCol");

            assertEquals(2, result.rows().size(), "Two countries: UK, USA");

            assertEquals("UK", result.rows().get(0).get(countryIdx));
            assertEquals(3000, ((Number) result.rows().get(0).get(aggIdx)).intValue(),
                    "UK 2011: LDN(3000)");

            assertEquals("USA", result.rows().get(1).get(countryIdx));
            assertEquals(7600, ((Number) result.rows().get(1).get(aggIdx)).intValue(),
                    "USA 2011: NYC(5000)+SAN(2600)=7600");
        }

        @Test
        @DisplayName("pivot → cast → filter on dynamic pivot column — verify exact surviving row")
        void testPivotCastFilterOnPivotCol() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        city, year, trees
                        NYC, 2011, 5000
                        NYC, 2012, 7600
                        SAN, 2011, 2000
                        SAN, 2012, 3000
                    #->pivot(~[year], ~[total:x|$x.trees:y|$y->plus()])
                    ->cast(@Relation<(city:String, '2011__|__total':Integer, '2012__|__total':Integer)>)
                    ->filter(x|$x.'2011__|__total' > 3000)
                    """);
            int cityIdx = columnIndex(result, "city");
            int y2011Idx = columnIndex(result, "2011__|__total");

            assertEquals(1, result.rows().size(), "Only NYC has 2011 total > 3000");
            assertEquals("NYC", result.rows().get(0).get(cityIdx));
            assertEquals(5000, ((Number) result.rows().get(0).get(y2011Idx)).intValue(),
                    "NYC 2011 total = 5000");
        }
    }

    // ==================== Cast on Class.all() sources ====================

    @Nested
    @DisplayName("cast on Class.all() — mapping context")
    class ClassCast {

        @Test
        @DisplayName("project → cast → groupBy max — verify aggregate per group")
        void testClassProjectCastAggregate() throws SQLException {
            // Test data: John(30), Jane(28), Bob(45) — two Smiths, one Jones
            var result = executeRelation("""
                    Person.all()
                        ->project(~[last:p|$p.lastName, age:p|$p.age])
                        ->cast(@Relation<(last:String, age:Integer)>)
                        ->groupBy(~[last], ~[maxAge:x|$x.age:y|$y->max()])
                        ->sort(~last->ascending())
                    """);
            int lastIdx = columnIndex(result, "last");
            int maxIdx = columnIndex(result, "maxAge");

            assertEquals(2, result.rows().size(), "Two last names: Jones, Smith");

            assertEquals("Jones", result.rows().get(0).get(lastIdx));
            assertEquals(45, ((Number) result.rows().get(0).get(maxIdx)).intValue(),
                    "Jones max age: Bob=45");

            assertEquals("Smith", result.rows().get(1).get(lastIdx));
            assertEquals(30, ((Number) result.rows().get(1).get(maxIdx)).intValue(),
                    "Smith max age: John=30 (Jane=28)");
        }

        @Test
        @DisplayName("project → cast schema subset — drops columns, verify remaining data")
        void testClassProjectCastDropColumns() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                        ->project(~[name:p|$p.firstName, age:p|$p.age])
                        ->cast(@Relation<(name:String)>)
                        ->sort(~name->ascending())
                    """);
            assertEquals(3, result.rows().size());
            assertEquals(1, result.columns().size(), "Only 'name' column survives cast");
            assertEquals("name", result.columns().get(0).name());
            assertFalse(hasColumn(result, "age"), "age dropped by cast");

            // Verify actual name values
            assertEquals("Bob", result.rows().get(0).get(0));
            assertEquals("Jane", result.rows().get(1).get(0));
            assertEquals("John", result.rows().get(2).get(0));
        }
    }

    // ==================== Schema identity & double cast ====================

    @Nested
    @DisplayName("cast identity — no-op and chaining")
    class IdentityCast {

        @Test
        @DisplayName("double cast narrows schema — verify only final columns and data survive")
        void testDoubleCastNarrows() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        name, age, city
                        Alice, 30, NYC
                        Bob, 45, LA
                    #->cast(@Relation<(name:String, age:Integer, city:String)>)
                    ->cast(@Relation<(name:String)>)
                    ->sort(~name->ascending())
                    """);
            assertEquals(2, result.rows().size());
            assertEquals(1, result.columns().size(), "Only 'name' survives double cast");
            assertEquals("name", result.columns().get(0).name());

            assertEquals("Alice", result.rows().get(0).get(0));
            assertEquals("Bob", result.rows().get(1).get(0));
        }

        @Test
        @DisplayName("identity cast then filter — verify exact filtered values")
        void testIdentityCastThenFilter() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        name, age
                        Alice, 30
                        Bob, 45
                        Charlie, 25
                    #->cast(@Relation<(name:String, age:Integer)>)
                    ->filter(x|$x.age > 30)
                    """);
            int nameIdx = columnIndex(result, "name");
            int ageIdx = columnIndex(result, "age");

            assertEquals(1, result.rows().size(), "Only Bob survives");
            assertEquals("Bob", result.rows().get(0).get(nameIdx));
            assertEquals(45, ((Number) result.rows().get(0).get(ageIdx)).intValue());
        }
    }

    // ==================== Quoted column names ====================

    @Nested
    @DisplayName("cast with quoted/special column names")
    class QuotedNames {

        @Test
        @DisplayName("pivot → cast with quoted cols → sort — verify order and all values")
        void testQuotedColumnSort() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        city, year, value
                        NYC, 2020, 300
                        SAN, 2020, 100
                        LA, 2020, 200
                    #->pivot(~[year], ~[amt:x|$x.value:y|$y->plus()])
                    ->cast(@Relation<(city:String, '2020__|__amt':Integer)>)
                    ->sort(~'2020__|__amt'->ascending())
                    """);
            int cityIdx = columnIndex(result, "city");
            int amtIdx = columnIndex(result, "2020__|__amt");

            assertEquals(3, result.rows().size());

            assertEquals("SAN", result.rows().get(0).get(cityIdx));
            assertEquals(100, ((Number) result.rows().get(0).get(amtIdx)).intValue());

            assertEquals("LA", result.rows().get(1).get(cityIdx));
            assertEquals(200, ((Number) result.rows().get(1).get(amtIdx)).intValue());

            assertEquals("NYC", result.rows().get(2).get(cityIdx));
            assertEquals(300, ((Number) result.rows().get(2).get(amtIdx)).intValue());
        }
    }

    // ==================== SQL generation ====================

    @Nested
    @DisplayName("cast SQL generation")
    class SqlGeneration {

        @Test
        @DisplayName("cast on TDS generates pass-through SQL — no CAST keyword")
        void testCastSqlNoKeyword() throws SQLException {
            String sql = generateSql("""
                    #TDS
                        name, age
                        Alice, 30
                    #->cast(@Relation<(name:String, age:Integer)>)
                    """);
            assertNotNull(sql);
            assertFalse(sql.isEmpty(), "SQL should not be empty");
            assertFalse(sql.toUpperCase().contains("CAST("),
                    "Relational cast is a type assertion, should not emit SQL CAST(): " + sql);
        }

        @Test
        @DisplayName("pivot → cast → filter generates SQL with PIVOT and dynamic column reference")
        void testPivotCastFilterSql() throws SQLException {
            String sql = generateSql("""
                    #TDS
                        city, year, trees
                        NYC, 2011, 5000
                        SAN, 2012, 2000
                    #->pivot(~[year], ~[total:x|$x.trees:y|$y->plus()])
                    ->cast(@Relation<(city:String, '2011__|__total':Integer, '2012__|__total':Integer)>)
                    ->filter(x|$x.'2011__|__total' > 1000)
                    """);
            assertNotNull(sql);
            assertTrue(sql.contains("PIVOT"), "Should contain PIVOT keyword: " + sql);
            assertTrue(sql.contains("2011"), "Should reference 2011 pivot column: " + sql);
            assertTrue(sql.contains("1000"), "Should contain filter value 1000: " + sql);
        }
    }

    // ==================== Utilities ====================

    private boolean hasColumn(ExecutionResult result, String name) {
        return result.columns().stream().anyMatch(c -> c.name().equals(name));
    }

    private int columnIndex(ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name()))
                return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }
}
