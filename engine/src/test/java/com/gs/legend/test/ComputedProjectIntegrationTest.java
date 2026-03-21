package com.gs.legend.test;

import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test reproducing the real PCT testSimpleProjectList failure.
 *
 * The source is a Collection of struct instances (^TypeForProjectTest(...)),
 * and compileCollection must propagate the struct's GenericType.Relation.Schema so that
 * compileProject can resolve columns.
 *
 * Without the fix, this fails with:
 *   NullPointerException: Cannot invoke "RelationType.columns()" because "sourceType" is null
 */
public class ComputedProjectIntegrationTest {

    /**
     * Class definitions matching the PCT test model.
     * These are the same classes that ExecuteLegendLiteQuery.extractClassMetadata
     * produces from the Pure interpreter.
     */
    private static final String MODEL = """
            Class meta::pure::functions::relation::tests::project::TypeForProjectTest {
                name: String[1];
                addresses: meta::pure::functions::relation::tests::project::Address[*];
                values: meta::pure::functions::relation::tests::project::PrimitiveContainer[*];
            }
            Class meta::pure::functions::relation::tests::project::Address {
                val: String[1];
            }
            Class meta::pure::functions::relation::tests::project::PrimitiveContainer {
                val: Integer[1];
            }
            Database store::TestDb ( Table T_DUMMY ( ID INTEGER ) )
            RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
            Runtime test::TestRuntime { mappings: []; connections: [ store::TestDb: [ environment: store::TestConn ] ]; }
            """;

    /**
     * The exact Pure expression from the failing PCT test testSimpleProjectList.
     * Copied verbatim from the [LegendLite PCT] log output.
     */
    private static final String PCT_EXPRESSION =
            "|[^meta::pure::functions::relation::tests::project::TypeForProjectTest(" +
            "name='ok', " +
            "addresses=[^meta::pure::functions::relation::tests::project::Address(val='no'), " +
            "^meta::pure::functions::relation::tests::project::Address(val='other')], " +
            "values=[^meta::pure::functions::relation::tests::project::PrimitiveContainer(val=1), " +
            "^meta::pure::functions::relation::tests::project::PrimitiveContainer(val=2), " +
            "^meta::pure::functions::relation::tests::project::PrimitiveContainer(val=3)]), " +
            "^meta::pure::functions::relation::tests::project::TypeForProjectTest(" +
            "name='ok3', " +
            "addresses=^meta::pure::functions::relation::tests::project::Address(val='no'), " +
            "values=[^meta::pure::functions::relation::tests::project::PrimitiveContainer(val=1), " +
            "^meta::pure::functions::relation::tests::project::PrimitiveContainer(val=2), " +
            "^meta::pure::functions::relation::tests::project::PrimitiveContainer(val=3)])]" +
            "->meta::pure::functions::relation::project(~[" +
            "one:x: meta::pure::functions::relation::tests::project::TypeForProjectTest[1]|$x.name," +
            "two:x: meta::pure::functions::relation::tests::project::TypeForProjectTest[1]|$x.addresses.val," +
            "three:x: meta::pure::functions::relation::tests::project::TypeForProjectTest[1]|$x.values.val])";

    @Test
    void testSimpleProjectList_pctExpression() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("SET timezone='UTC'");
            }

            QueryService qs = new QueryService();
            var result = qs.execute(MODEL, PCT_EXPRESSION, "test::TestRuntime", connection);

            assertNotNull(result, "Result should not be null");
            assertEquals(3, result.columns().size(), "Should have 3 columns: one, two, three");
            assertEquals("one", result.columns().get(0).name());
            assertEquals("two", result.columns().get(1).name());
            assertEquals("three", result.columns().get(2).name());

            // First TypeForProjectTest: 2 addresses × 3 values = 6 rows
            // Second TypeForProjectTest: 1 address × 3 values = 3 rows
            // Total: 9 rows
            assertTrue(result.rows().size() > 0, "Should have rows");
            System.out.println("Rows: " + result.rows().size());
            for (var row : result.rows()) {
            System.out.println("  " + row.values());
            }
        }
    }

    // ==================== PCT testFilterPostProject ====================

    /**
     * Model for the PCT composition tests (testFilterPostProject etc).
     */
    private static final String COMPOSITION_MODEL = """
            Class meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests {
                firstName: String[1];
                lastName: String[1];
            }
            Class meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests {
                legalName: String[1];
                employees: meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests[*];
            }
            Database store::TestDb ( Table T_DUMMY ( ID INTEGER ) )
            RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
            Runtime test::TestRuntime { mappings: []; connections: [ store::TestDb: [ environment: store::TestConn ] ]; }
            """;

    /**
     * Exact PCT testFilterPostProject expression.
     * [^FirmType(...)...]->project(~[legalName, firstName])->filter(x|$x.legalName == 'Firm X')
     */
    @Test
    void testFilterPostProject() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("SET timezone='UTC'");
            }

            QueryService qs = new QueryService();
            var result = qs.execute(
                    COMPOSITION_MODEL,
                    "|[^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(legalName='Firm X', employees=[^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName='Peter', lastName='Smith'), ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName='John', lastName='Johnson'), ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName='John', lastName='Hill'), ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName='Anthony', lastName='Allen')]), ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(legalName='Firm A', employees=^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName='Fabrice', lastName='Roberts'))]->"
                    + "meta::pure::functions::relation::project(~[legalName:x: meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests[1]|$x.legalName,firstName:x: meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests[1]|$x.employees.firstName])->"
                    + "meta::pure::functions::relation::filter(x: (legalName:String, firstName:String)[1]|$x.legalName == 'Firm X')",
                    "test::TestRuntime", connection);

            assertNotNull(result, "Result should not be null");
            assertEquals(2, result.columns().size(), "Should have 2 columns: legalName, firstName");
            assertEquals("legalName", result.columns().get(0).name());
            assertEquals("firstName", result.columns().get(1).name());

            // PCT asserts 4 rows (sorted by firstName ascending):
            //   Firm X, Anthony
            //   Firm X, John
            //   Firm X, John
            //   Firm X, Peter
            var rows = result.rows();
            assertEquals(4, rows.size(), "Should have 4 rows for Firm X employees");

            // Sort by firstName for deterministic ordering (matches PCT ->sort(~firstName->ascending()))
            rows.sort((a, b) -> String.valueOf(a.values().get(1)).compareTo(String.valueOf(b.values().get(1))));

            assertEquals("Firm X", rows.get(0).values().get(0));
            assertEquals("Anthony", rows.get(0).values().get(1));
            assertEquals("Firm X", rows.get(1).values().get(0));
            assertEquals("John", rows.get(1).values().get(1));
            assertEquals("Firm X", rows.get(2).values().get(0));
            assertEquals("John", rows.get(2).values().get(1));
            assertEquals("Firm X", rows.get(3).values().get(0));
            assertEquals("Peter", rows.get(3).values().get(1));
        }
    }

    // ==================== PCT testGroupByCastBeforeAgg / AfterAgg ====================

    private static final String TDS_MODEL = """
            Database store::TestDb ( Table T_DUMMY ( ID INTEGER ) )
            RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
            Runtime test::TestRuntime { mappings: []; connections: [ store::TestDb: [ environment: store::TestConn ] ]; }
            """;

    /**
     * PCT testGroupByCastBeforeAgg: cast(@Integer) wraps the values BEFORE plus().
     * Pattern: groupBy(~grp, ~newCol:x|$x.id:x|+($x->cast(@Integer)))
     *
     * Expected (sorted by grp ascending):
     *   grp=0→10, grp=1→16, grp=2→6, grp=3→10, grp=4→4, grp=5→9
     */
    @Test
    void testGroupByCastBeforeAgg() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("SET timezone='UTC'");
            }

            QueryService qs = new QueryService();
            var result = qs.execute(
                    TDS_MODEL,
                    "|#TDS\nid, grp\n1, 2\n2, 1\n3, 3\n4, 4\n5, 2\n6, 1\n7, 3\n8, 1\n9, 5\n10, 0\n#->"
                    + "meta::pure::functions::relation::groupBy(~grp, "
                    + "~newCol:x: (id:Integer, grp:Integer)[1]|$x.id:"
                    + "x: Integer[*]|+($x->meta::pure::functions::lang::cast(@Integer)))",
                    "test::TestRuntime", connection);

            assertNotNull(result, "Result should not be null");
            assertEquals(2, result.columns().size(), "Should have 2 columns: grp, newCol");
            assertEquals("grp", result.columns().get(0).name());
            assertEquals("newCol", result.columns().get(1).name());

            var rows = result.rows();
            assertEquals(6, rows.size(), "Should have 6 groups");

            // Sort by grp for deterministic ordering
            rows.sort((a, b) -> Integer.compare(
                    ((Number) a.values().get(0)).intValue(),
                    ((Number) b.values().get(0)).intValue()));

            // PCT expected: grp=0→10, 1→16, 2→6, 3→10, 4→4, 5→9
            assertEquals(0, ((Number) rows.get(0).values().get(0)).intValue());
            assertEquals(10, ((Number) rows.get(0).values().get(1)).intValue());
            assertEquals(1, ((Number) rows.get(1).values().get(0)).intValue());
            assertEquals(16, ((Number) rows.get(1).values().get(1)).intValue());
            assertEquals(2, ((Number) rows.get(2).values().get(0)).intValue());
            assertEquals(6, ((Number) rows.get(2).values().get(1)).intValue());
            assertEquals(3, ((Number) rows.get(3).values().get(0)).intValue());
            assertEquals(10, ((Number) rows.get(3).values().get(1)).intValue());
            assertEquals(4, ((Number) rows.get(4).values().get(0)).intValue());
            assertEquals(4, ((Number) rows.get(4).values().get(1)).intValue());
            assertEquals(5, ((Number) rows.get(5).values().get(0)).intValue());
            assertEquals(9, ((Number) rows.get(5).values().get(1)).intValue());
        }
    }

    /**
     * PCT testGroupByCastAfterAgg: cast(@Integer) wraps the result AFTER plus().
     * Pattern: groupBy(~grp, ~newCol:x|$x.id:x|+($x)->cast(@Integer))
     *
     * Same expected results (cast is a no-op on Integer).
     */
    @Test
    void testGroupByCastAfterAgg() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("SET timezone='UTC'");
            }

            QueryService qs = new QueryService();
            var result = qs.execute(
                    TDS_MODEL,
                    "|#TDS\nid, grp\n1, 2\n2, 1\n3, 3\n4, 4\n5, 2\n6, 1\n7, 3\n8, 1\n9, 5\n10, 0\n#->"
                    + "meta::pure::functions::relation::groupBy(~grp, "
                    + "~newCol:x: (id:Integer, grp:Integer)[1]|$x.id:"
                    + "x: Integer[*]|+($x)->meta::pure::functions::lang::cast(@Integer))",
                    "test::TestRuntime", connection);

            assertNotNull(result, "Result should not be null");
            assertEquals(2, result.columns().size(), "Should have 2 columns: grp, newCol");

            var rows = result.rows();
            assertEquals(6, rows.size(), "Should have 6 groups");

            rows.sort((a, b) -> Integer.compare(
                    ((Number) a.values().get(0)).intValue(),
                    ((Number) b.values().get(0)).intValue()));

            // Same expected results — cast is a no-op
            assertEquals(0, ((Number) rows.get(0).values().get(0)).intValue());
            assertEquals(10, ((Number) rows.get(0).values().get(1)).intValue());
            assertEquals(1, ((Number) rows.get(1).values().get(0)).intValue());
            assertEquals(16, ((Number) rows.get(1).values().get(1)).intValue());
            assertEquals(2, ((Number) rows.get(2).values().get(0)).intValue());
            assertEquals(6, ((Number) rows.get(2).values().get(1)).intValue());
            assertEquals(3, ((Number) rows.get(3).values().get(0)).intValue());
            assertEquals(10, ((Number) rows.get(3).values().get(1)).intValue());
            assertEquals(4, ((Number) rows.get(4).values().get(0)).intValue());
            assertEquals(4, ((Number) rows.get(4).values().get(1)).intValue());
            assertEquals(5, ((Number) rows.get(5).values().get(0)).intValue());
            assertEquals(9, ((Number) rows.get(5).values().get(1)).intValue());
        }
    }

    // ==================== PCT testOLAPAggCastWithPartitionWindow ====================

    private static final String TDS_3COL_MODEL = """
            Database store::TestDb ( Table T_DUMMY ( ID INTEGER ) )
            RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
            Runtime test::TestRuntime { mappings: []; connections: [ store::TestDb: [ environment: store::TestConn ] ]; }
            """;

    /**
     * PCT testOLAPAggCastWithPartitionWindow:
     *   extend(~grp->over(), ~newCol:{p,w,r|$r.id}:y|+($y->cast(@Integer)))
     *
     * The +() prefix is lost by the parser (signedExpression rule), leaving
     * cast($y, @Integer) as the aggregate body. extractPureFuncName must
     * default to "plus" so this gets compiled as SUM(id) OVER (PARTITION BY grp).
     *
     * Expected: each row gets the SUM of id within its grp partition.
     *   grp=0: sum=10, grp=1: sum=16, grp=2: sum=6, grp=3: sum=10, grp=4: sum=4, grp=5: sum=9
     */
    @Test
    void testOLAPAggCastWithPartitionWindow() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("SET timezone='UTC'");
            }

            QueryService qs = new QueryService();
            var result = qs.execute(
                    TDS_3COL_MODEL,
                    "|#TDS\nid, grp, name\n1, 2, A\n2, 1, B\n3, 3, C\n4, 4, D\n5, 2, E\n6, 1, F\n7, 3, G\n8, 1, H\n9, 5, I\n10, 0, J\n#->"
                    + "meta::pure::functions::relation::extend("
                    + "~grp->meta::pure::functions::relation::over(), "
                    + "~newCol:{p: meta::pure::metamodel::relation::Relation<(id:Integer, grp:Integer, name:String)>[1], "
                    + "w: meta::pure::functions::relation::_Window<(id:Integer, grp:Integer, name:String)>[1], "
                    + "r: (id:Integer, grp:Integer, name:String)[1]|$r.id}"
                    + ":y: Integer[*]|+($y->meta::pure::functions::lang::cast(@Integer)))",
                    "test::TestRuntime", connection);

            assertNotNull(result, "Result should not be null");
            // Should have original 3 columns + newCol
            assertTrue(result.columns().size() >= 4, "Should have at least 4 columns: id, grp, name, newCol");

            var rows = result.rows();
            assertEquals(10, rows.size(), "Should have 10 rows (one per input row)");

            // Find newCol column index
            int newColIdx = -1;
            for (int i = 0; i < result.columns().size(); i++) {
                if ("newCol".equals(result.columns().get(i).name())) {
                    newColIdx = i;
                    break;
                }
            }
            assertTrue(newColIdx >= 0, "Should have newCol column");

            // Verify window aggregate: each row's newCol = SUM(id) within its grp
            // Build expected sums by grp: 0->10, 1->16, 2->6, 3->10, 4->4, 5->9
            int grpIdx = 1; // grp is second column
            for (var row : rows) {
                int grp = ((Number) row.values().get(grpIdx)).intValue();
                int newCol = ((Number) row.values().get(newColIdx)).intValue();
                int expectedSum = switch (grp) {
                    case 0 -> 10;
                    case 1 -> 16;
                    case 2 -> 6;
                    case 3 -> 10;
                    case 4 -> 4;
                    case 5 -> 9;
                    default -> throw new AssertionError("Unexpected grp: " + grp);
                };
                assertEquals(expectedSum, newCol,
                        "Row with grp=" + grp + " should have newCol=" + expectedSum);
            }
        }
    }

    // ==================== PCT testSimpleRelationProject ====================

    /**
     * Reproduces the PCT testSimpleRelationProject failure.
     *
     * Pure expression:
     *   #TDS val, str 1, a 3, ewe ...#
     *     ->project(~[name:c:(val:Integer, str:String)[1]|$c.str->toOne() + $c.val->toOne()->toString()])
     *
     * This is a PURELY COMPUTED column — 'name' doesn't exist in source columns [val, str].
     * The type must be inferred from the lambda body (string concat → String).
     *
     * Without fix, fails with:
     *   PureCompileException: project(): cannot resolve type for column 'name'.
     *   Not found in source columns [val, str]
     */
    @Test
    void testSimpleRelationProject_computedColumn() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("SET timezone='UTC'");
            }

            QueryService qs = new QueryService();
            var result = qs.execute(
                    TDS_MODEL,
                    "|#TDS\nval, str\n1, a\n3, ewe\n4, qw\n5, wwe\n6, weq\n#->"
                    + "meta::pure::functions::relation::project("
                    + "~[name:c: (val:Integer, str:String)[1]"
                    + "|$c.str->meta::pure::functions::multiplicity::toOne()"
                    + " + $c.val->meta::pure::functions::multiplicity::toOne()"
                    + "->meta::pure::functions::string::toString()])",
                    "test::TestRuntime", connection);

            assertNotNull(result, "Result should not be null");
            assertEquals(1, result.columns().size(), "Should have 1 column: name");
            assertEquals("name", result.columns().get(0).name());

            var rows = result.rows();
            assertEquals(5, rows.size(), "Should have 5 rows");

            // Expected: str + toString(val) = "a1", "ewe3", "qw4", "wwe5", "weq6"
            assertEquals("a1", rows.get(0).values().get(0));
            assertEquals("ewe3", rows.get(1).values().get(0));
            assertEquals("qw4", rows.get(2).values().get(0));
            assertEquals("wwe5", rows.get(3).values().get(0));
            assertEquals("weq6", rows.get(4).values().get(0));
        }
    }
}
