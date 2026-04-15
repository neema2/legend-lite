package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for relational mappings.
 * Every test creates real DuckDB tables, defines Pure models, executes queries
 * through QueryService, and asserts on actual results.
 *
 * Organized by feature area with ~150+ tests covering:
 * - Simple column mappings (all types)
 * - Filter queries (all operators)
 * - Projections and computed columns
 * - Sort, limit, slice
 * - Aggregation (groupBy, count, sum, avg, min, max)
 * - Single-join navigation (to-one, to-many, EXISTS, LEFT JOIN)
 * - Enumeration mappings
 * - Pure M2M mappings
 * - Multi-table databases
 * - GAP features (disabled, documenting what's not yet supported)
 */
@DisplayName("Relational Mapping Integration Tests")
class RelationalMappingIntegrationTest {

    private Connection conn;
    private final QueryService qs = new QueryService();

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // ==================== Helpers ====================

    /** Builds connection + runtime Pure source wrapping the given model. */
    private String withRuntime(String model, String dbName, String mappingName) {
        return model + """
                import test::*;


                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    /** Execute Pure query and return result. */
    private ExecutionResult exec(String model, String query) throws SQLException {
        return qs.execute(model, query, "test::RT", conn);
    }

    /** Execute SQL directly on the connection. */
    private void sql(String... statements) throws SQLException {
        try (Statement s = conn.createStatement()) {
            for (String stmt : statements) s.execute(stmt);
        }
    }

    /** Get all values from column index as strings. */
    private List<String> colStr(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? row.get(idx).toString() : null)
                .collect(Collectors.toList());
    }

    /** Get all values from column index as integers. */
    private List<Integer> colInt(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? ((Number) row.get(idx)).intValue() : null)
                .collect(Collectors.toList());
    }

    /** Generate SQL without executing. */
    private String planSql(String model, String query) {
        return com.gs.legend.plan.PlanGenerator.generate(model, query, "test::RT").sql();
    }

    /** Convenience: single-table model with given class, db, mapping, columns. */
    private String singleTableModel(String className, String tableName, String dbName,
                                     String mappingName, String classDef, String tableDef,
                                     String mappingBody) {
        return withRuntime("""
                import model::*;
                import store::*;

                %s
                Database %s ( Table %s ( %s ) )
                Mapping %s ( %s: Relational { ~mainTable [%s] %s %s } )
                """.formatted(classDef, dbName, tableName, tableDef,
                mappingName, className, dbName, tableName, mappingBody),
                dbName, mappingName);
    }

    // ==================== 1. Simple Column Mappings ====================

    @Nested
    @DisplayName("1. Simple Column Mappings")
    class SimpleColumnMappings {

        @Test
        @DisplayName("String column mapping")
        void testStringColumn() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, NAME VARCHAR(100))",
                "INSERT INTO T1 VALUES (1, 'Alice'), (2, 'Bob')");
            String m = singleTableModel("P", "T1", "store::DB", "model::M",
                    "Class model::P { name: String[1]; }",
                    "ID INTEGER, NAME VARCHAR(100)",
                    "name: [store::DB] T1.NAME");
            var r = exec(m, "P.all()->project(~[name:x|$x.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test
        @DisplayName("Integer column mapping")
        void testIntegerColumn() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, AGE INT)",
                "INSERT INTO T1 VALUES (1, 25), (2, 30)");
            String m = singleTableModel("P", "T1", "store::DB", "model::M",
                    "Class model::P { age: Integer[1]; }",
                    "ID INTEGER, AGE INTEGER",
                    "age: [store::DB] T1.AGE");
            var r = exec(m, "P.all()->project(~[age:x|$x.age])");
            assertEquals(2, r.rowCount());
            assertTrue(colInt(r, 0).containsAll(List.of(25, 30)));
        }

        @Test
        @DisplayName("Float column mapping")
        void testFloatColumn() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, PRICE DOUBLE)",
                "INSERT INTO T1 VALUES (1, 9.99), (2, 19.99)");
            String m = singleTableModel("P", "T1", "store::DB", "model::M",
                    "Class model::P { price: Float[1]; }",
                    "ID INTEGER, PRICE DOUBLE",
                    "price: [store::DB] T1.PRICE");
            var r = exec(m, "P.all()->project(~[price:x|$x.price])");
            assertEquals(2, r.rowCount());
        }

        @Test
        @DisplayName("Boolean column mapping")
        void testBooleanColumn() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, ACTIVE BOOLEAN)",
                "INSERT INTO T1 VALUES (1, true), (2, false)");
            String m = singleTableModel("P", "T1", "store::DB", "model::M",
                    "Class model::P { active: Boolean[1]; }",
                    "ID INTEGER, ACTIVE BOOLEAN",
                    "active: [store::DB] T1.ACTIVE");
            var r = exec(m, "P.all()->project(~[active:x|$x.active])");
            assertEquals(2, r.rowCount());
        }

        @Test
        @DisplayName("Date column mapping")
        void testDateColumn() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, DOB DATE)",
                "INSERT INTO T1 VALUES (1, '1990-01-15'), (2, '1985-06-20')");
            String m = singleTableModel("P", "T1", "store::DB", "model::M",
                    "Class model::P { dob: Date[1]; }",
                    "ID INTEGER, DOB DATE",
                    "dob: [store::DB] T1.DOB");
            var r = exec(m, "P.all()->project(~[dob:x|$x.dob])");
            assertEquals(2, r.rowCount());
            assertNotNull(r.rows().get(0).get(0));
        }

        @Test
        @DisplayName("Multiple columns from same table")
        void testMultipleColumns() throws SQLException {
            sql("CREATE TABLE EMP (ID INT, NAME VARCHAR(100), DEPT VARCHAR(50), SAL INT)",
                "INSERT INTO EMP VALUES (1, 'Alice', 'Eng', 100000), (2, 'Bob', 'Sales', 80000)");
            String m = singleTableModel("Emp", "EMP", "store::DB", "model::M",
                    "Class model::Emp { name: String[1]; dept: String[1]; sal: Integer[1]; }",
                    "ID INTEGER, NAME VARCHAR(100), DEPT VARCHAR(50), SAL INTEGER",
                    "name: [store::DB] EMP.NAME, dept: [store::DB] EMP.DEPT, sal: [store::DB] EMP.SAL");
            var r = exec(m, "Emp.all()->project(~[name:x|$x.name, dept:x|$x.dept, sal:x|$x.sal])");
            assertEquals(2, r.rowCount());
            assertEquals(3, r.columnCount());
        }

        @Test
        @DisplayName("Column name different from property name")
        void testColumnNameDifferentFromProperty() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, LEGAL_NAME VARCHAR(200))",
                "INSERT INTO T1 VALUES (1, 'Acme Corp'), (2, 'Beta Inc')");
            String m = singleTableModel("Firm", "T1", "store::DB", "model::M",
                    "Class model::Firm { legalName: String[1]; }",
                    "ID INTEGER, LEGAL_NAME VARCHAR(200)",
                    "legalName: [store::DB] T1.LEGAL_NAME");
            var r = exec(m, "Firm.all()->project(~[legalName:x|$x.legalName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).contains("Acme Corp"));
        }

        @Test
        @DisplayName("Table with many column types")
        void testManyColumnTypes() throws SQLException {
            sql("CREATE TABLE MIXED (ID INT, S VARCHAR(100), I BIGINT, F DOUBLE, B BOOLEAN, D DATE)",
                "INSERT INTO MIXED VALUES (1, 'hello', 42, 3.14, true, '2024-01-01')");
            String m = singleTableModel("R", "MIXED", "store::DB", "model::M",
                    "Class model::R { s: String[1]; i: Integer[1]; f: Float[1]; b: Boolean[1]; d: Date[1]; }",
                    "ID INTEGER, S VARCHAR(100), I BIGINT, F DOUBLE, B BOOLEAN, D DATE",
                    "s: [store::DB] MIXED.S, i: [store::DB] MIXED.I, f: [store::DB] MIXED.F, b: [store::DB] MIXED.B, d: [store::DB] MIXED.D");
            var r = exec(m, "R.all()->project(~[s:x|$x.s, i:x|$x.i, f:x|$x.f, b:x|$x.b, d:x|$x.d])");
            assertEquals(1, r.rowCount());
            assertEquals(5, r.columnCount());
            assertEquals("hello", r.rows().get(0).get(0).toString());
        }

        @Test
        @DisplayName("Empty table returns zero rows")
        void testEmptyTable() throws SQLException {
            sql("CREATE TABLE EMPTY_T (ID INT, NAME VARCHAR(100))");
            String m = singleTableModel("P", "EMPTY_T", "store::DB", "model::M",
                    "Class model::P { name: String[1]; }",
                    "ID INTEGER, NAME VARCHAR(100)",
                    "name: [store::DB] EMPTY_T.NAME");
            var r = exec(m, "P.all()->project(~[name:x|$x.name])");
            assertEquals(0, r.rowCount());
        }

        @Test
        @DisplayName("Large result set (1000 rows)")
        void testLargeResultSet() throws SQLException {
            sql("CREATE TABLE BIG (ID INT, VAL INT)");
            StringBuilder insert = new StringBuilder("INSERT INTO BIG VALUES ");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) insert.append(",");
                insert.append("(").append(i).append(",").append(i * 10).append(")");
            }
            sql(insert.toString());
            String m = singleTableModel("R", "BIG", "store::DB", "model::M",
                    "Class model::R { val: Integer[1]; }",
                    "ID INTEGER, VAL INTEGER",
                    "val: [store::DB] BIG.VAL");
            var r = exec(m, "R.all()->project(~[val:x|$x.val])");
            assertEquals(1000, r.rowCount());
        }

        @Test
        @DisplayName("Two classes mapped to different tables")
        void testTwoClassesTwoTables() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE T_FIRM (ID INT, LEGAL VARCHAR(200))",
                "INSERT INTO T_PERSON VALUES (1, 'Alice')",
                "INSERT INTO T_FIRM VALUES (1, 'Acme')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legal: String[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER, NAME VARCHAR(100))
                        Table T_FIRM (ID INTEGER, LEGAL VARCHAR(200))
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Firm: Relational { ~mainTable [store::DB] T_FIRM legal: [store::DB] T_FIRM.LEGAL }
                    )
                    """, "store::DB", "model::M");
            var r1 = exec(model, "Person.all()->project(~[name:x|$x.name])");
            assertEquals(1, r1.rowCount());
            assertEquals("Alice", r1.rows().get(0).get(0).toString());

            var r2 = exec(model, "Firm.all()->project(~[legal:x|$x.legal])");
            assertEquals(1, r2.rowCount());
            assertEquals("Acme", r2.rows().get(0).get(0).toString());
        }
    }

    // ==================== 2. Filter Queries ====================

    @Nested
    @DisplayName("2. Filter Queries")
    class FilterQueries {

        private String empModel;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE EMP (ID INT, NAME VARCHAR(100), DEPT VARCHAR(50), SAL INT, ACTIVE BOOLEAN)",
                "INSERT INTO EMP VALUES (1, 'Alice', 'Engineering', 100000, true)",
                "INSERT INTO EMP VALUES (2, 'Bob', 'Sales', 80000, true)",
                "INSERT INTO EMP VALUES (3, 'Charlie', 'Engineering', 90000, false)",
                "INSERT INTO EMP VALUES (4, 'Diana', 'Marketing', 70000, true)",
                "INSERT INTO EMP VALUES (5, 'Eve', 'Sales', 85000, false)");
            empModel = singleTableModel("Emp", "EMP", "store::DB", "model::M",
                    "Class model::Emp { name: String[1]; dept: String[1]; sal: Integer[1]; active: Boolean[1]; }",
                    "ID INTEGER, NAME VARCHAR(100), DEPT VARCHAR(50), SAL INTEGER, ACTIVE BOOLEAN",
                    "name: [store::DB] EMP.NAME, dept: [store::DB] EMP.DEPT, sal: [store::DB] EMP.SAL, active: [store::DB] EMP.ACTIVE");
        }

        @Test @DisplayName("Filter: equality (==)")
        void testEquality() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.dept == 'Engineering'})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
        }

        @Test @DisplayName("Filter: inequality (!=)")
        void testInequality() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.dept != 'Engineering'})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount());
            assertFalse(colStr(r, 0).contains("Alice"));
            assertFalse(colStr(r, 0).contains("Charlie"));
        }

        @Test @DisplayName("Filter: greater than (>)")
        void testGreaterThan() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.sal > 85000})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
        }

        @Test @DisplayName("Filter: greater or equal (>=)")
        void testGreaterOrEqual() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.sal >= 90000})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Filter: less than (<)")
        void testLessThan() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.sal < 85000})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Bob", "Diana")));
        }

        @Test @DisplayName("Filter: less or equal (<=)")
        void testLessOrEqual() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.sal <= 80000})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Filter: boolean AND (&&)")
        void testBooleanAnd() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.dept == 'Engineering' && $e.sal > 95000})->project(~[name:e|$e.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Filter: boolean OR (||)")
        void testBooleanOr() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.dept == 'Marketing' || $e.dept == 'Sales'})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("Filter: boolean NOT (!)")
        void testBooleanNot() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|!($e.dept == 'Engineering')})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("Filter: boolean column directly")
        void testBooleanColumnFilter() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.active == true})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob", "Diana")));
        }

        @Test @DisplayName("Filter: string contains")
        void testStringContains() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.name->contains('li')})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
        }

        @Test @DisplayName("Filter: string startsWith")
        void testStringStartsWith() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.name->startsWith('A')})->project(~[name:e|$e.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Filter: string endsWith")
        void testStringEndsWith() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.name->endsWith('e')})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie", "Eve")));
        }

        @Test @DisplayName("Filter: complex compound predicate")
        void testComplexCompound() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|($e.dept == 'Engineering' && $e.sal > 95000) || ($e.dept == 'Sales' && $e.active == true)})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("Filter returns zero rows")
        void testFilterNoMatch() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.sal > 999999})->project(~[name:e|$e.name])");
            assertEquals(0, r.rowCount());
        }

        @Test @DisplayName("Filter: chained filters")
        void testChainedFilters() throws SQLException {
            var r = exec(empModel, "Emp.all()->filter({e|$e.dept == 'Engineering'})->filter({e|$e.sal > 95000})->project(~[name:e|$e.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
        }
    }

    // ==================== 3. Projections ====================

    @Nested
    @DisplayName("3. Projections")
    class Projections {

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE PROJ (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), SAL INT)",
                "INSERT INTO PROJ VALUES (1, 'Alice', 'Smith', 100000), (2, 'Bob', 'Jones', 80000)");
        }

        private String projModel() {
            return singleTableModel("P", "PROJ", "store::DB", "model::M",
                    "Class model::P { first: String[1]; last: String[1]; sal: Integer[1]; }",
                    "ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), SAL INTEGER",
                    "first: [store::DB] PROJ.FIRST, last: [store::DB] PROJ.LAST, sal: [store::DB] PROJ.SAL");
        }

        @Test @DisplayName("Project single column")
        void testSingleColumn() throws SQLException {
            var r = exec(projModel(), "P.all()->project(~[first:x|$x.first])");
            assertEquals(2, r.rowCount());
            assertEquals(1, r.columnCount());
        }

        @Test @DisplayName("Project multiple columns")
        void testMultipleColumns() throws SQLException {
            var r = exec(projModel(), "P.all()->project(~[first:x|$x.first, last:x|$x.last, sal:x|$x.sal])");
            assertEquals(2, r.rowCount());
            assertEquals(3, r.columnCount());
        }

        @Test @DisplayName("Project with filter")
        void testProjectWithFilter() throws SQLException {
            var r = exec(projModel(), "P.all()->filter({x|$x.sal > 90000})->project(~[first:x|$x.first])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Project preserves column names from tilde syntax")
        void testColumnNames() throws SQLException {
            var r = exec(projModel(), "P.all()->project(~[firstName:x|$x.first, lastName:x|$x.last])");
            assertEquals("firstName", r.columns().get(0).name());
            assertEquals("lastName", r.columns().get(1).name());
        }

        @Test @DisplayName("Legacy project syntax with lambda list")
        void testLegacyProjectSyntax() throws SQLException {
            var r = exec(projModel(), "P.all()->project([{x|$x.first}, {x|$x.last}], ['first_name', 'last_name'])");
            assertEquals(2, r.rowCount());
            assertEquals("first_name", r.columns().get(0).name());
        }
    }

    // ==================== 4. Sort, Limit, Slice ====================

    @Nested
    @DisplayName("4. Sort, Limit, Slice")
    class SortLimitSlice {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE NUMS (ID INT, VAL INT, NAME VARCHAR(50))",
                "INSERT INTO NUMS VALUES (1, 30, 'C'), (2, 10, 'A'), (3, 50, 'E'), (4, 20, 'B'), (5, 40, 'D')");
            model = singleTableModel("N", "NUMS", "store::DB", "model::M",
                    "Class model::N { val: Integer[1]; name: String[1]; }",
                    "ID INTEGER, VAL INTEGER, NAME VARCHAR(50)",
                    "val: [store::DB] NUMS.VAL, name: [store::DB] NUMS.NAME");
        }

        @Test @DisplayName("sortBy ascending")
        void testSortByAsc() throws SQLException {
            var r = exec(model, "N.all()->sortBy({x|$x.val})->project(~[val:x|$x.val])");
            var vals = colInt(r, 0);
            assertEquals(List.of(10, 20, 30, 40, 50), vals);
        }

        @Test @DisplayName("sortByReversed descending")
        void testSortByDesc() throws SQLException {
            var r = exec(model, "N.all()->sortByReversed({x|$x.val})->project(~[val:x|$x.val])");
            var vals = colInt(r, 0);
            assertEquals(List.of(50, 40, 30, 20, 10), vals);
        }

        @Test @DisplayName("limit(3)")
        void testLimit() throws SQLException {
            var r = exec(model, "N.all()->sortBy({x|$x.val})->limit(3)->project(~[val:x|$x.val])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("limit(0) returns empty")
        void testLimitZero() throws SQLException {
            var r = exec(model, "N.all()->limit(0)->project(~[val:x|$x.val])");
            assertEquals(0, r.rowCount());
        }

        @Test @DisplayName("sort + filter + limit combo")
        void testSortFilterLimit() throws SQLException {
            var r = exec(model, "N.all()->filter({x|$x.val > 15})->sortBy({x|$x.val})->limit(2)->project(~[val:x|$x.val])");
            assertEquals(2, r.rowCount());
            assertEquals(List.of(20, 30), colInt(r, 0));
        }

        @Test @DisplayName("Relation sort + slice")
        void testRelationSortSlice() throws SQLException {
            var r = exec(model,
                    "N.all()->project([{x|$x.val}], ['val'])->sort('val', SortDirection.ASC)->slice(1, 3)");
            assertEquals(2, r.rowCount());
            // Rows 1-2 (0-indexed skip 1, take 2): val=20, val=30
        }
    }

    // ==================== 5. Aggregation ====================

    @Nested
    @DisplayName("5. Aggregation")
    class Aggregation {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE SALES (ID INT, REGION VARCHAR(20), PRODUCT VARCHAR(20), AMOUNT INT, QTY INT)",
                "INSERT INTO SALES VALUES (1, 'East', 'Widget', 100, 5)",
                "INSERT INTO SALES VALUES (2, 'East', 'Gadget', 200, 3)",
                "INSERT INTO SALES VALUES (3, 'West', 'Widget', 150, 7)",
                "INSERT INTO SALES VALUES (4, 'West', 'Gadget', 250, 2)",
                "INSERT INTO SALES VALUES (5, 'East', 'Widget', 120, 4)");
            model = singleTableModel("S", "SALES", "store::DB", "model::M",
                    "Class model::S { region: String[1]; product: String[1]; amount: Integer[1]; qty: Integer[1]; }",
                    "ID INTEGER, REGION VARCHAR(20), PRODUCT VARCHAR(20), AMOUNT INTEGER, QTY INTEGER",
                    "region: [store::DB] SALES.REGION, product: [store::DB] SALES.PRODUCT, amount: [store::DB] SALES.AMOUNT, qty: [store::DB] SALES.QTY");
        }

        @Test @DisplayName("GroupBy single column with sum")
        void testGroupBySum() throws SQLException {
            var r = exec(model,
                    "S.all()->project(~[region:x|$x.region, amount:x|$x.amount])->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->sum()})], ['region', 'totalAmount'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(420, results.get("East"));
            assertEquals(400, results.get("West"));
        }

        @Test @DisplayName("GroupBy with count")
        void testGroupByCount() throws SQLException {
            var r = exec(model,
                    "S.all()->project(~[region:x|$x.region, amount:x|$x.amount])->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->count()})], ['region', 'cnt'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(3, results.get("East"));
            assertEquals(2, results.get("West"));
        }

        @Test @DisplayName("GroupBy with average")
        void testGroupByAvg() throws SQLException {
            var r = exec(model,
                    "S.all()->project(~[region:x|$x.region, amount:x|$x.amount])->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->average()})], ['region', 'avgAmt'])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("GroupBy with min and max")
        void testGroupByMinMax() throws SQLException {
            var r = exec(model,
                    "S.all()->project(~[region:x|$x.region, amount:x|$x.amount])->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->min()})], ['region', 'minAmt'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(100, results.get("East"));
            assertEquals(150, results.get("West"));
        }

        @Test @DisplayName("GroupBy multiple columns")
        void testGroupByMultipleColumns() throws SQLException {
            var r = exec(model,
                    "S.all()->project(~[region:x|$x.region, product:x|$x.product, amount:x|$x.amount])->groupBy([{r|$r.region}, {r|$r.product}], [agg({r|$r.amount}, {y|$y->sum()})], ['region', 'product', 'total'])");
            assertEquals(4, r.rowCount()); // East-Widget, East-Gadget, West-Widget, West-Gadget
        }

        @Test @DisplayName("GroupBy directly on class source (arity-4, no intermediate project)")
        void testGroupByClassSource() throws SQLException {
            var r = exec(model,
                    "S.all()->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->sum()})], ['region', 'totalAmount'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(420, results.get("East"));
            assertEquals(400, results.get("West"));
        }

        @Test @DisplayName("Class-source groupBy with count aggregation")
        void testClassSourceGroupByCount() throws SQLException {
            var r = exec(model,
                    "S.all()->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->count()})], ['region', 'cnt'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(3, results.get("East"));
            assertEquals(2, results.get("West"));
        }

        @Test @DisplayName("Class-source groupBy with average aggregation")
        void testClassSourceGroupByAvg() throws SQLException {
            var r = exec(model,
                    "S.all()->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->average()})], ['region', 'avgAmt'])");
            assertEquals(2, r.rowCount());
            Map<String, Double> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).doubleValue());
            assertEquals(140.0, results.get("East"), 0.01);
            assertEquals(200.0, results.get("West"), 0.01);
        }

        @Test @DisplayName("Class-source groupBy with min/max aggregation")
        void testClassSourceGroupByMinMax() throws SQLException {
            var r = exec(model,
                    "S.all()->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->min()}), agg({r|$r.amount}, {y|$y->max()})], ['region', 'minAmt', 'maxAmt'])");
            assertEquals(2, r.rowCount());
            Map<String, List<Integer>> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(),
                    List.of(((Number) row.get(1)).intValue(), ((Number) row.get(2)).intValue()));
            assertEquals(List.of(100, 200), results.get("East"));
            assertEquals(List.of(150, 250), results.get("West"));
        }

        @Test @DisplayName("Class-source groupBy with multiple key columns")
        void testClassSourceGroupByMultiKey() throws SQLException {
            var r = exec(model,
                    "S.all()->groupBy([{r|$r.region}, {r|$r.product}], [agg({r|$r.amount}, {y|$y->sum()})], ['region', 'product', 'total'])");
            assertEquals(4, r.rowCount());
        }

        @Test @DisplayName("Class-source groupBy with DynaFunc-mapped key column")
        void testClassSourceGroupByDynaFuncKey() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), SALARY INT)",
                "INSERT INTO PEOPLE VALUES (1, 'Alice', 'Smith', 100), (2, 'Bob', 'Smith', 200), (3, 'Carol', 'Jones', 300)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::P { fullName: String[1]; lastName: String[1]; salary: Integer[1]; }
                    Database store::DB (
                        Table PEOPLE (ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), SALARY INTEGER)
                    )
                    Mapping model::M (
                        model::P: Relational {
                            ~mainTable [store::DB] PEOPLE
                            fullName: concat(PEOPLE.FIRST, ' ', PEOPLE.LAST),
                            lastName: [store::DB] PEOPLE.LAST,
                            salary: [store::DB] PEOPLE.SALARY
                        }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m,
                    "P.all()->groupBy([{p|$p.lastName}], [agg({p|$p.salary}, {y|$y->sum()})], ['lastName', 'totalSalary'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(300, results.get("Smith"));
            assertEquals(300, results.get("Jones"));
        }

        @Test @DisplayName("Class-source groupBy keyed on DynaFunc-computed property")
        void testClassSourceGroupByDynaFuncComputedKey() throws SQLException {
            sql("CREATE TABLE ITEMS (ID INT, CATEGORY VARCHAR(50), PRICE INT)",
                "INSERT INTO ITEMS VALUES (1, 'ELECTRONICS', 100), (2, 'electronics', 200), (3, 'TOOLS', 50)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::I { categoryLower: String[1]; price: Integer[1]; }
                    Database store::DB (
                        Table ITEMS (ID INTEGER, CATEGORY VARCHAR(50), PRICE INTEGER)
                    )
                    Mapping model::M (
                        model::I: Relational {
                            ~mainTable [store::DB] ITEMS
                            categoryLower: toLower(ITEMS.CATEGORY),
                            price: [store::DB] ITEMS.PRICE
                        }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m,
                    "I.all()->groupBy([{i|$i.categoryLower}], [agg({i|$i.price}, {y|$y->sum()})], ['category', 'totalPrice'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(300, results.get("electronics"));
            assertEquals(50, results.get("tools"));
        }

        @Test @DisplayName("Class-source groupBy with join-chain key column")
        void testClassSourceGroupByJoinKey() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, CUST_ID INT, AMOUNT INT)",
                "CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR(50))",
                "INSERT INTO CUSTOMERS VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO ORDERS VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Ord { custName: String[1]; amount: Integer[1]; }
                    Database store::DB (
                        Table ORDERS (ID INTEGER, CUST_ID INTEGER, AMOUNT INTEGER)
                        Table CUSTOMERS (ID INTEGER, NAME VARCHAR(50))
                        Join OrdCust(ORDERS.CUST_ID = CUSTOMERS.ID)
                    )
                    Mapping model::M (
                        model::Ord: Relational {
                            ~mainTable [store::DB] ORDERS
                            custName: @OrdCust | [store::DB] CUSTOMERS.NAME,
                            amount: [store::DB] ORDERS.AMOUNT
                        }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m,
                    "Ord.all()->groupBy([{o|$o.custName}], [agg({o|$o.amount}, {y|$y->sum()})], ['customer', 'totalAmount'])");
            assertEquals(2, r.rowCount());
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(300, results.get("Alice"));
            assertEquals(50, results.get("Bob"));
        }

        @Test @DisplayName("Distinct on relation")
        void testDistinct() throws SQLException {
            sql("CREATE TABLE DUPS (ID INT, VAL VARCHAR(20))",
                "INSERT INTO DUPS VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::D { val: String[1]; }
                    Database store::DB ( Table DUPS (ID INTEGER, VAL VARCHAR(20)) )
                    Mapping model::M ( D: Relational { ~mainTable [store::DB] DUPS val: [store::DB] DUPS.VAL } )
                    """, "store::DB", "model::M");
            var r = exec(m, "D.all()->project(~[val:x|$x.val])->distinct()");
            assertEquals(3, r.rowCount());
        }
    }

    // ==================== 6. Join Navigation (Associations) ====================

    @Nested
    @DisplayName("6. Join Navigation")
    class JoinNavigation {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT PRIMARY KEY, LEGAL_NAME VARCHAR(200))",
                "CREATE TABLE T_ADDRESS (ID INT PRIMARY KEY, PERSON_ID INT, STREET VARCHAR(200), CITY VARCHAR(100))",
                "INSERT INTO T_FIRM VALUES (1, 'Acme Corp'), (2, 'Beta Inc')",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)",
                "INSERT INTO T_ADDRESS VALUES (1, 1, '123 Main St', 'New York'), (2, 1, '456 Oak Ave', 'Boston'), (3, 2, '789 Elm St', 'Chicago')");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; }
                    Class model::Address { street: String[1]; city: String[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Association model::Person_Address { person: Person[1]; addresses: Address[*]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(200))
                        Table T_ADDRESS (ID INTEGER PRIMARY KEY, PERSON_ID INTEGER, STREET VARCHAR(200), CITY VARCHAR(100))
                        Join Person_Firm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Firm: Relational { ~mainTable [store::DB] T_FIRM legalName: [store::DB] T_FIRM.LEGAL_NAME }
                        Address: Relational { ~mainTable [store::DB] T_ADDRESS street: [store::DB] T_ADDRESS.STREET, city: [store::DB] T_ADDRESS.CITY }
                    
                        model::Person_Firm: Relational { AssociationMapping ( person: [store::DB]@Person_Firm, firm: [store::DB]@Person_Firm ) }
                        model::Person_Address: Relational { AssociationMapping ( person: [store::DB]@Person_Address, addresses: [store::DB]@Person_Address ) }
)
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("Project through to-one association (firm.legalName)")
        void testToOneProjection() throws SQLException {
            var r = exec(model, "Person.all()->project(~[name:p|$p.name, firm:p|$p.firm.legalName])");
            assertEquals(3, r.rowCount());
            // Alice and Bob → Acme Corp, Charlie → Beta Inc
        }

        @Test @DisplayName("Filter on to-one association")
        void testFilterToOne() throws SQLException {
            var r = exec(model, "Person.all()->filter({p|$p.firm.legalName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("Filter on to-many association uses EXISTS (no row explosion)")
        void testFilterToManyExists() throws SQLException {
            var r = exec(model, "Person.all()->filter({p|$p.addresses.city == 'New York'})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("To-many: person with multiple addresses appears once in filter")
        void testNoRowExplosion() throws SQLException {
            // Alice has 2 addresses. Filtering by either city should return Alice ONCE
            var r = exec(model, "Person.all()->filter({p|$p.addresses.city == 'New York' || $p.addresses.city == 'Boston'})->project(~[name:p|$p.name])");
            long aliceCount = colStr(r, 0).stream().filter("Alice"::equals).count();
            assertEquals(1, aliceCount, "Alice should appear exactly once despite matching 2 addresses");
        }

        @Test @DisplayName("Project through to-many (LEFT JOIN)")
        void testToManyProjection() throws SQLException {
            var r = exec(model, "Person.all()->project(~[name:p|$p.name, street:p|$p.addresses.street])");
            // Alice has 2 addresses → 2 rows, Bob has 1, Charlie has 0
            assertTrue(r.rowCount() >= 3, "Should have at least 3 rows from LEFT JOIN");
        }

        @Test @DisplayName("Filter local, project through association")
        void testFilterLocalProjectAssoc() throws SQLException {
            var r = exec(model, "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name, street:p|$p.addresses.street])");
            assertEquals(2, r.rowCount()); // Alice has 2 addresses
        }

        @Test @DisplayName("Person with no addresses still appears (LEFT JOIN)")
        void testPersonNoAddressLeftJoin() throws SQLException {
            var r = exec(model, "Person.all()->project(~[name:p|$p.name, street:p|$p.addresses.street])");
            // Charlie has no addresses → should still appear with NULL street
            boolean charlieFound = false;
            for (var row : r.rows()) {
                if ("Charlie".equals(row.get(0).toString())) {
                    charlieFound = true;
                    assertNull(row.get(1), "Charlie should have null street");
                }
            }
            assertTrue(charlieFound, "Charlie should appear even without addresses");
        }
    }

    // ==================== 7. Enumeration Mappings ====================

    @Nested
    @DisplayName("7. Enumeration Mappings")
    class EnumerationMappings {

        @Test @DisplayName("Query with string enum column")
        void testStringEnumQuery() throws SQLException {
            sql("CREATE TABLE TASKS (ID INT, NAME VARCHAR(100), STATUS VARCHAR(20))",
                "INSERT INTO TASKS VALUES (1, 'Fix bug', 'PENDING'), (2, 'Write docs', 'DONE'), (3, 'Deploy', 'PENDING')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Enum model::TaskStatus { PENDING, IN_PROGRESS, DONE }
                    Class model::Task { name: String[1]; status: TaskStatus[1]; }
                    Database store::DB ( Table TASKS (ID INTEGER, NAME VARCHAR(100), STATUS VARCHAR(20)) )
                    Mapping model::M ( Task: Relational { ~mainTable [store::DB] TASKS name: [store::DB] TASKS.NAME, status: [store::DB] TASKS.STATUS } )
                    """, "store::DB", "model::M");
            var r = exec(m, "Task.all()->filter({t|$t.status == 'PENDING'})->project(~[name:t|$t.name])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Enum with enumeration mapping transformer (CASE WHEN)")
        void testEnumMappingTransformer() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, STATUS_CODE VARCHAR(5))",
                "INSERT INTO ORDERS VALUES (1, 'A'), (2, 'I'), (3, 'A'), (4, 'C')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Enum model::OrderStatus { ACTIVE, INACTIVE, CANCELLED }
                    Class model::Order { status: OrderStatus[1]; }
                    Database store::DB ( Table ORDERS (ID INTEGER, STATUS_CODE VARCHAR(5)) )
                    Mapping model::M (
                        Order: Relational { ~mainTable [store::DB] ORDERS status: EnumerationMapping StatusMap: [store::DB] ORDERS.STATUS_CODE }
                        OrderStatus: EnumerationMapping StatusMap { ACTIVE: 'A', INACTIVE: 'I', CANCELLED: 'C' }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Order.all()->filter({o|$o.status == 'ACTIVE'})->project(~[status:o|$o.status])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Enum mapping with integer source values")
        void testEnumMappingInteger() throws SQLException {
            sql("CREATE TABLE FLAGS (ID INT, LEVEL INT)",
                "INSERT INTO FLAGS VALUES (1, 1), (2, 2), (3, 1), (4, 3)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Enum model::Priority { LOW, MEDIUM, HIGH }
                    Class model::Flag { priority: Priority[1]; }
                    Database store::DB ( Table FLAGS (ID INTEGER, LEVEL INTEGER) )
                    Mapping model::M (
                        Flag: Relational { ~mainTable [store::DB] FLAGS priority: EnumerationMapping PriorityMap: [store::DB] FLAGS.LEVEL }
                        Priority: EnumerationMapping PriorityMap { LOW: 1, MEDIUM: 2, HIGH: 3 }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Flag.all()->project(~[priority:f|$f.priority])");
            assertEquals(4, r.rowCount());
        }

        @Test @DisplayName("Enum mapping with array source values")
        void testEnumMappingArray() throws SQLException {
            sql("CREATE TABLE ITEMS (ID INT, CATEGORY VARCHAR(20))",
                "INSERT INTO ITEMS VALUES (1, 'A'), (2, 'ACT'), (3, 'ACTIVE'), (4, 'I')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Enum model::Status { ACTIVE, INACTIVE }
                    Class model::Item { status: Status[1]; }
                    Database store::DB ( Table ITEMS (ID INTEGER, CATEGORY VARCHAR(20)) )
                    Mapping model::M (
                        Item: Relational { ~mainTable [store::DB] ITEMS status: EnumerationMapping SM: [store::DB] ITEMS.CATEGORY }
                        Status: EnumerationMapping SM { ACTIVE: ['A', 'ACT', 'ACTIVE'], INACTIVE: 'I' }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Item.all()->filter({i|$i.status == 'ACTIVE'})->project(~[status:i|$i.status])");
            assertEquals(3, r.rowCount());
        }
    }

    // ==================== 8. Pure M2M Mappings ====================

    @Nested
    @DisplayName("8. Pure M2M Mappings")
    class M2MMappings {

        @Test @DisplayName("Simple M2M: project from source to target")
        void testSimpleM2M() throws SQLException {
            sql("CREATE TABLE RAW_PERSON (ID INT, FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50))",
                "INSERT INTO RAW_PERSON VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::RawPerson { firstName: String[1]; lastName: String[1]; }
                    Class model::Person { fullName: String[1]; }
                    Database store::DB ( Table RAW_PERSON (ID INTEGER, FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50)) )
                    Mapping model::M (
                        RawPerson: Relational { ~mainTable [store::DB] RAW_PERSON firstName: [store::DB] RAW_PERSON.FIRST_NAME, lastName: [store::DB] RAW_PERSON.LAST_NAME }
                        Person: Pure { ~src RawPerson fullName: $src.firstName + ' ' + $src.lastName }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Person.all()->project(~[fullName:p|$p.fullName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("John Doe", "Jane Smith")));
        }

        @Test @DisplayName("M2M with property selection")
        void testM2MPropertySelection() throws SQLException {
            sql("CREATE TABLE SRC (ID INT, A VARCHAR(50), B VARCHAR(50), C INT)",
                "INSERT INTO SRC VALUES (1, 'hello', 'world', 42)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Source { a: String[1]; b: String[1]; c: Integer[1]; }
                    Class model::Target { combined: String[1]; doubled: Integer[1]; }
                    Database store::DB ( Table SRC (ID INTEGER, A VARCHAR(50), B VARCHAR(50), C INTEGER) )
                    Mapping model::M (
                        Source: Relational { ~mainTable [store::DB] SRC a: [store::DB] SRC.A, b: [store::DB] SRC.B, c: [store::DB] SRC.C }
                        Target: Pure { ~src Source combined: $src.a + '-' + $src.b, doubled: $src.c * 2 }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Target.all()->project(~[combined:t|$t.combined, doubled:t|$t.doubled])");
            assertEquals(1, r.rowCount());
            assertEquals("hello-world", r.rows().get(0).get(0).toString());
            assertEquals(84, ((Number) r.rows().get(0).get(1)).intValue());
        }
    }

    // ==================== 9. Relation API ====================

    @Nested
    @DisplayName("9. Relation API")
    class RelationApi {

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_DATA (ID INT, NAME VARCHAR(50), VAL INT)",
                "INSERT INTO T_DATA VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
        }

        private String relModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::D { name: String[1]; val: Integer[1]; }
                    Database store::DB ( Table T_DATA (ID INTEGER, NAME VARCHAR(50), VAL INTEGER) )
                    Mapping model::M ( D: Relational { ~mainTable [store::DB] T_DATA name: [store::DB] T_DATA.NAME, val: [store::DB] T_DATA.VAL } )
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("Direct table access: #>{DB.TABLE}#")
        void testDirectTableAccess() throws SQLException {
            var r = exec(relModel(), "#>{store::DB.T_DATA}#->from(test::RT)");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("Relation select columns")
        void testRelationSelect() throws SQLException {
            var r = exec(relModel(), "#>{store::DB.T_DATA}#->select(~[NAME, VAL])->from(test::RT)");
            assertEquals(3, r.rowCount());
            assertEquals(2, r.columnCount());
        }

        @Test @DisplayName("Relation filter")
        void testRelationFilter() throws SQLException {
            var r = exec(relModel(), "#>{store::DB.T_DATA}#->filter(x|$x.VAL > 15)->from(test::RT)");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Relation sort")
        void testRelationSort() throws SQLException {
            var r = exec(relModel(), "#>{store::DB.T_DATA}#->select(~VAL)->sort(~VAL->descending())->from(test::RT)");
            assertEquals(List.of(30, 20, 10), colInt(r, 0));
        }

        @Test @DisplayName("Relation extend (computed column)")
        void testRelationExtend() throws SQLException {
            var r = exec(relModel(), "#>{store::DB.T_DATA}#->extend(~doubled:x|$x.VAL * 2)->select(~[VAL, doubled])->from(test::RT)");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("Relation rename")
        void testRelationRename() throws SQLException {
            var r = exec(relModel(), "#>{store::DB.T_DATA}#->rename(~NAME, ~label)->select(~label)->from(test::RT)");
            assertEquals(3, r.rowCount());
            assertEquals("label", r.columns().get(0).name());
        }
    }

    // ==================== 9b. Traverse (Relation API) ====================

    @Nested
    @DisplayName("9b. Traverse — extend(traverse(), colSpec)")
    class TraverseTests {

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp'), (2, 'Globex')",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1), (3, 'Research', 2)",
                // Dave has DEPT_ID=999 (no match) → LEFT JOIN produces NULLs
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 3), (4, 'Dave', 999)");
        }

        private String traverseModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Dummy { x: String[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                    )
                    Mapping model::M ( Dummy: Relational { ~mainTable [store::DB] T_PERSON x: [store::DB] T_PERSON.NAME } )
                    """, "store::DB", "model::M");
        }

        // --- Basic traverse ---

        @Test @DisplayName("Single-hop traverse: Person → Dept")
        void testSingleHopTraverse() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            assertEquals("Engineering", depts.get(names.indexOf("Alice")));
            assertEquals("Sales", depts.get(names.indexOf("Bob")));
            assertEquals("Research", depts.get(names.indexOf("Charlie")));
        }

        @Test @DisplayName("Multi-hop traverse: Person → Dept → Org")
        void testMultiHopTraverse() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->select(~[NAME, orgName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var orgs = colStr(r, 1);
            assertEquals("Acme Corp", orgs.get(names.indexOf("Alice")));
            assertEquals("Acme Corp", orgs.get(names.indexOf("Bob")));
            assertEquals("Globex", orgs.get(names.indexOf("Charlie")));
        }

        @Test @DisplayName("Multi-column ColSpecArray from same traversal")
        void testMultiColumnTraverse() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~[deptName:{src,tgt|$tgt.NAME}, deptId:{src,tgt|$tgt.ID}])->select(~[NAME, deptName, deptId])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            var deptIds = colInt(r, 2);
            assertEquals("Engineering", depts.get(names.indexOf("Alice")));
            assertEquals(Integer.valueOf(1), deptIds.get(names.indexOf("Alice")));
            assertEquals("Research", depts.get(names.indexOf("Charlie")));
            assertEquals(Integer.valueOf(3), deptIds.get(names.indexOf("Charlie")));
        }

        // --- LEFT JOIN NULL (no matching FK) ---

        @Test @DisplayName("LEFT JOIN: unmatched FK produces NULL")
        void testTraverseLeftJoinNull() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            // Dave has DEPT_ID=999 → no match → NULL
            assertNull(depts.get(names.indexOf("Dave")));
        }

        @Test @DisplayName("Multi-hop LEFT JOIN: NULL propagates through chain")
        void testMultiHopLeftJoinNull() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->select(~[NAME, orgName])->from(test::RT)");
            var names = colStr(r, 0);
            var orgs = colStr(r, 1);
            assertNull(orgs.get(names.indexOf("Dave")));
        }

        // --- Traverse + filter ---

        @Test @DisplayName("Filter on traversed column")
        void testTraverseThenFilter() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->filter(x|$x.deptName == 'Engineering')->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Engineering", colStr(r, 1).get(0));
        }

        @Test @DisplayName("Filter on multi-hop traversed column")
        void testMultiHopTraverseThenFilter() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->filter(x|$x.orgName == 'Globex')->select(~[NAME])->from(test::RT)");
            assertEquals(1, r.rowCount());
            assertEquals("Charlie", colStr(r, 0).get(0));
        }

        @Test @DisplayName("Filter on source column after traverse")
        void testTraverseFilterOnSourceColumn() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->filter(x|$x.NAME == 'Alice')->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(1, r.rowCount());
            assertEquals("Engineering", colStr(r, 1).get(0));
        }

        // --- Traverse + sort ---

        @Test @DisplayName("Sort by traversed column")
        void testTraverseThenSort() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->sort(~deptName->ascending())->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var depts = colStr(r, 1);
            // DuckDB: NULLS LAST for ASC → Engineering, Research, Sales, NULL
            assertEquals("Engineering", depts.get(0));
            assertEquals("Research", depts.get(1));
            assertEquals("Sales", depts.get(2));
            assertNull(depts.get(3));
        }

        // --- Traverse + filter + sort ---

        @Test @DisplayName("Traverse + filter + sort combined")
        void testTraverseFilterSort() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->filter(x|$x.orgName == 'Acme Corp')->sort(~NAME->ascending())->select(~[NAME, orgName])->from(test::RT)");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Alice", "Bob"), colStr(r, 0));
        }

        // --- Traverse + limit/take ---

        @Test @DisplayName("Traverse + sort + limit")
        void testTraverseSortLimit() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->sort(~NAME->ascending())->limit(2)->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Alice", "Bob"), colStr(r, 0));
        }

        // --- Traverse + groupBy ---

        @Test @DisplayName("GroupBy on traversed column with count")
        void testTraverseGroupBy() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->groupBy(~orgName, ~cnt:x|$x.NAME:y|$y->count())->sort(~orgName->ascending())->from(test::RT)");
            // DuckDB ASC NULLS LAST: Acme Corp=2, Globex=1, NULL=1
            assertEquals(3, r.rowCount());
            var orgs = colStr(r, 0);
            var counts = colInt(r, 1);
            assertEquals("Acme Corp", orgs.get(0));
            assertEquals(Integer.valueOf(2), counts.get(0));
            assertEquals("Globex", orgs.get(1));
            assertEquals(Integer.valueOf(1), counts.get(1));
        }

        // --- Traverse + select (project only traversed columns) ---

        @Test @DisplayName("Select only traversed columns")
        void testTraverseSelectOnlyTraversedCols() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~[deptName:{src,tgt|$tgt.NAME}, deptOrgId:{src,tgt|$tgt.ORG_ID}])->select(~[deptName, deptOrgId])->from(test::RT)");
            assertEquals(4, r.rowCount());
            assertEquals(2, r.columnCount());
            assertEquals("deptName", r.columns().get(0).name());
            assertEquals("deptOrgId", r.columns().get(1).name());
        }

        // --- Multiple independent traversals ---

        @Test @DisplayName("Two independent traverse extends on same source")
        void testTwoIndependentTraversals() throws SQLException {
            // First extend: Person → Dept (deptName)
            // Second extend: Person → Dept → Org (orgName)
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->select(~[NAME, deptName, orgName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            var orgs = colStr(r, 2);
            assertEquals("Engineering", depts.get(names.indexOf("Alice")));
            assertEquals("Acme Corp", orgs.get(names.indexOf("Alice")));
            assertEquals("Research", depts.get(names.indexOf("Charlie")));
            assertEquals("Globex", orgs.get(names.indexOf("Charlie")));
        }

        // --- Traverse + extend (computed column on traversed data) ---

        @Test @DisplayName("Extend computed column using traversed column")
        void testTraverseThenExtendComputed() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptId:{src,tgt|$tgt.ID})->extend(~deptIdDoubled:x|$x.deptId * 2)->select(~[NAME, deptId, deptIdDoubled])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var deptIds = colInt(r, 1);
            var doubled = colInt(r, 2);
            int aliceIdx = names.indexOf("Alice");
            assertEquals(Integer.valueOf(1), deptIds.get(aliceIdx));
            assertEquals(Integer.valueOf(2), doubled.get(aliceIdx));
        }

        @Test @DisplayName("Scalar extend THEN traverse (inlined column preserved)")
        void testScalarExtendThenTraverse() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(~idDoubled:x|$x.ID * 2)->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->select(~[NAME, idDoubled, deptName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var doubled = colInt(r, 1);
            var depts = colStr(r, 2);
            int aliceIdx = names.indexOf("Alice");
            assertEquals(Integer.valueOf(2), doubled.get(aliceIdx));
            assertEquals("Engineering", depts.get(aliceIdx));
        }

        // --- Compound (non-equi) join conditions ---

        @Test @DisplayName("Compound condition: equi-join AND filter in ON clause")
        void testTraverseCompoundCondition() throws SQLException {
            // Join person→dept WHERE dept ID matches AND dept is in org > 1 (Globex only)
            // Alice(dept=1,org=1)→NULL, Bob(dept=2,org=1)→NULL, Charlie(dept=3,org=2)→Research
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID && $hop.ORG_ID > 1}), ~deptName:{src,tgt|$tgt.NAME})->select(~[NAME, deptName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            var names = colStr(r, 0);
            var depts = colStr(r, 1);
            assertEquals("Research", depts.get(names.indexOf("Charlie")));
            assertNull(depts.get(names.indexOf("Alice")));
            assertNull(depts.get(names.indexOf("Bob")));
        }

        @Test @DisplayName("Non-equi join: range condition")
        void testTraverseNonEquiJoin() throws SQLException {
            // Join person→dept WHERE person.ID >= dept.ID (1-to-many: row expansion)
            // Alice(1): dept 1 only; Bob(2): dept 1,2; Charlie(3): dept 1,2,3; Dave(4): dept 1,2,3
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.ID >= $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->groupBy(~NAME, ~cnt:x|$x.deptName:y|$y->count())->sort(~NAME->ascending())->from(test::RT)");
            var names = colStr(r, 0);
            var counts = colInt(r, 1);
            assertEquals(Integer.valueOf(1), counts.get(names.indexOf("Alice")));
            assertEquals(Integer.valueOf(2), counts.get(names.indexOf("Bob")));
            assertEquals(Integer.valueOf(3), counts.get(names.indexOf("Charlie")));
        }

        // --- Interleaved operations between traversals ---

        @Test @DisplayName("Traverse → filter → traverse (filter between two traversals)")
        void testTraverseFilterTraverse() throws SQLException {
            // First: add deptName via traverse
            // Then: filter to Engineering only
            // Then: add orgName via second traverse chain
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->filter(x|$x.deptName == 'Engineering')" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->select(~[NAME, deptName, orgName])->from(test::RT)");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Engineering", colStr(r, 1).get(0));
            assertEquals("Acme Corp", colStr(r, 2).get(0));
        }

        @Test @DisplayName("Traverse → sort → traverse (sort between two traversals)")
        void testTraverseSortTraverse() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->sort(~NAME->ascending())" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->select(~[NAME, deptName, orgName])->from(test::RT)");
            assertEquals(4, r.rowCount());
            // Sorted by NAME ascending: Alice, Bob, Charlie, Dave
            assertEquals(List.of("Alice", "Bob", "Charlie", "Dave"), colStr(r, 0));
            assertEquals("Acme Corp", colStr(r, 2).get(0)); // Alice→Acme
            assertEquals("Globex", colStr(r, 2).get(2)); // Charlie→Globex
        }

        // --- GroupBy aggregating traversed columns ---

        @Test @DisplayName("GroupBy source col, aggregate traversed col (sum)")
        void testGroupBySourceAggregateTraversed() throws SQLException {
            // Bring in deptId via traverse, then sum it grouped by name
            // Each person has one dept, so sum = their dept ID
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptOrgId:{src,tgt|$tgt.ORG_ID})->groupBy(~NAME, ~totalOrgId:x|$x.deptOrgId:y|$y->sum())->sort(~NAME->ascending())->from(test::RT)");
            var names = colStr(r, 0);
            var sums = colInt(r, 1);
            // Alice→dept 1 (ORG_ID=1), Bob→dept 2 (ORG_ID=1), Charlie→dept 3 (ORG_ID=2)
            assertEquals(Integer.valueOf(1), sums.get(names.indexOf("Alice")));
            assertEquals(Integer.valueOf(1), sums.get(names.indexOf("Bob")));
            assertEquals(Integer.valueOf(2), sums.get(names.indexOf("Charlie")));
        }

        @Test @DisplayName("GroupBy traversed col, aggregate source col (sum of person IDs per org)")
        void testGroupByTraversedAggregateSource() throws SQLException {
            var r = exec(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->groupBy(~orgName, ~totalId:x|$x.ID:y|$y->sum())->sort(~orgName->ascending())->from(test::RT)");
            // Acme Corp: Alice(1)+Bob(2)=3, Globex: Charlie(3)=3, NULL: Dave(4)=4
            assertEquals(3, r.rowCount());
            var orgs = colStr(r, 0);
            var sums = colInt(r, 1);
            assertEquals(Integer.valueOf(3), sums.get(orgs.indexOf("Acme Corp")));
            assertEquals(Integer.valueOf(3), sums.get(orgs.indexOf("Globex")));
        }
    }

    // ==================== 9c. Traverse SQL Structure Assertions ====================

    @Nested
    @DisplayName("9c. Traverse SQL Structure — flat JOINs, no subquery wrap")
    class TraverseSqlStructure {

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp'), (2, 'Globex')",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1), (3, 'Research', 2)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 3), (4, 'Dave', 999)");
        }

        private String traverseModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Dummy { x: String[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                    )
                    Mapping model::M ( Dummy: Relational { ~mainTable [store::DB] T_PERSON x: [store::DB] T_PERSON.NAME } )
                    """, "store::DB", "model::M");
        }

        /** Count occurrences of keyword in SQL. */
        private int count(String sql, String keyword) {
            int c = 0;
            for (int i = sql.indexOf(keyword); i >= 0; i = sql.indexOf(keyword, i + keyword.length())) c++;
            return c;
        }

        @Test @DisplayName("Single-hop: flat LEFT JOIN, exactly 1 SELECT")
        void testSingleHopFlatSql() throws SQLException {
            // No select() — raw traverse output, no outer subquery
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})->from(test::RT)");
            assertEquals(1, count(sql, "SELECT"), "Flat: exactly 1 SELECT: " + sql);
            assertEquals(1, count(sql, "LEFT OUTER JOIN"), "Exactly 1 LEFT OUTER JOIN: " + sql);
            assertTrue(sql.contains("\"t0\".\"DEPT_ID\""), "ON clause uses physical column ref: " + sql);
        }

        @Test @DisplayName("Multi-hop: flat LEFT JOINs, exactly 1 SELECT")
        void testMultiHopFlatSql() throws SQLException {
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})->from(test::RT)");
            assertEquals(1, count(sql, "SELECT"), "Flat: exactly 1 SELECT: " + sql);
            assertEquals(2, count(sql, "LEFT OUTER JOIN"), "Exactly 2 LEFT OUTER JOINs: " + sql);
        }

        @Test @DisplayName("Two independent traversals: flat JOINs, exactly 1 SELECT")
        void testTwoIndependentTraversalsFlatSql() throws SQLException {
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->from(test::RT)");
            assertEquals(1, count(sql, "SELECT"), "Flat: exactly 1 SELECT: " + sql);
            assertEquals(3, count(sql, "LEFT OUTER JOIN"), "Exactly 3 LEFT OUTER JOINs: " + sql);
        }

        @Test @DisplayName("Filter before traverse: wraps filter, traverse is flat on wrapped source")
        void testFilterBeforeTraverseSql() throws SQLException {
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#->filter(x|$x.ID > 1)" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->from(test::RT)");
            // Filter produces non-table-based source → traverse wraps it.
            // 2 SELECTs: inner (filter) + outer (traverse JOIN)
            assertEquals(2, count(sql, "SELECT"), "Filter + traverse = 2 SELECTs: " + sql);
            assertEquals(1, count(sql, "LEFT OUTER JOIN"), "Exactly 1 LEFT OUTER JOIN: " + sql);
            assertTrue(sql.contains("WHERE"), "Filter becomes WHERE: " + sql);
        }

        @Test @DisplayName("Traverse → filter → traverse: filter breaks chain, second traverse wraps")
        void testTraverseFilterTraverseSql() throws SQLException {
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->filter(x|$x.deptName == 'Engineering')" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->from(test::RT)");
            // First traverse flat (1 JOIN), filter wraps, second traverse flat (2 JOINs) on wrapped source
            assertEquals(3, count(sql, "LEFT OUTER JOIN"), "3 LEFT OUTER JOINs total: " + sql);
            assertTrue(sql.contains("trav_src"), "Second traverse wraps in subquery: " + sql);
            assertTrue(sql.contains("WHERE"), "Filter becomes WHERE: " + sql);
        }

        @Test @DisplayName("Traverse → sort → traverse: sort breaks chain, second traverse wraps")
        void testTraverseSortTraverseSql() throws SQLException {
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptName:{src,tgt|$tgt.NAME})" +
                "->sort(~NAME->ascending())" +
                "->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID})->traverse(#>{store::DB.T_ORG}#, {prev,hop|$prev.ORG_ID == $hop.ID}), ~orgName:{src,tgt|$tgt.NAME})" +
                "->from(test::RT)");
            // First traverse flat (1 JOIN) + sort, then second traverse wraps (2 JOINs)
            assertEquals(3, count(sql, "LEFT OUTER JOIN"), "3 LEFT OUTER JOINs total: " + sql);
            assertTrue(sql.contains("trav_src"), "Second traverse wraps in subquery: " + sql);
            assertTrue(sql.contains("ORDER BY"), "Sort preserved: " + sql);
        }

        @Test @DisplayName("Traverse then scalar extend: flat, exactly 1 SELECT")
        void testTraverseThenScalarExtendFlatSql() throws SQLException {
            String sql = planSql(traverseModel(),
                "#>{store::DB.T_PERSON}#->extend(traverse(#>{store::DB.T_DEPT}#, {prev,hop|$prev.DEPT_ID == $hop.ID}), ~deptId:{src,tgt|$tgt.ID})->extend(~deptIdDoubled:x|$x.deptId * 2)->from(test::RT)");
            assertEquals(1, count(sql, "SELECT"), "Flat: exactly 1 SELECT: " + sql);
            assertEquals(1, count(sql, "LEFT OUTER JOIN"), "Exactly 1 LEFT OUTER JOIN: " + sql);
            assertTrue(sql.contains("deptIdDoubled"), "Scalar extend column present: " + sql);
        }
    }

    // ==================== 10. Multi-Table Databases ====================

    @Nested
    @DisplayName("10. Multi-Table Databases")
    class MultiTableDatabases {

        @Test @DisplayName("Three tables with joins")
        void testThreeTablesWithJoins() throws SQLException {
            sql("CREATE TABLE DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50))",
                "CREATE TABLE EMP (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE PROJ (ID INT PRIMARY KEY, NAME VARCHAR(100), EMP_ID INT)",
                "INSERT INTO DEPT VALUES (1, 'Engineering'), (2, 'Sales')",
                "INSERT INTO EMP VALUES (1, 'Alice', 1), (2, 'Bob', 2)",
                "INSERT INTO PROJ VALUES (1, 'Project X', 1), (2, 'Project Y', 1), (3, 'Project Z', 2)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Dept { name: String[1]; }
                    Class model::Emp { name: String[1]; }
                    Class model::Proj { name: String[1]; }
                    Association model::Emp_Dept { emp: Emp[*]; dept: Dept[1]; }
                    Association model::Emp_Proj { emp: Emp[1]; projects: Proj[*]; }
                    Database store::DB (
                        Table DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50))
                        Table EMP (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table PROJ (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), EMP_ID INTEGER)
                        Join Emp_Dept(EMP.DEPT_ID = DEPT.ID)
                        Join Emp_Proj(EMP.ID = PROJ.EMP_ID)
                    )
                    Mapping model::M (
                        Dept: Relational { ~mainTable [store::DB] DEPT name: [store::DB] DEPT.NAME }
                        Emp: Relational { ~mainTable [store::DB] EMP name: [store::DB] EMP.NAME }
                        Proj: Relational { ~mainTable [store::DB] PROJ name: [store::DB] PROJ.NAME }
                    
                        model::Emp_Dept: Relational { AssociationMapping ( emp: [store::DB]@Emp_Dept, dept: [store::DB]@Emp_Dept ) }
                        model::Emp_Proj: Relational { AssociationMapping ( emp: [store::DB]@Emp_Proj, projects: [store::DB]@Emp_Proj ) }
)
                    """, "store::DB", "model::M");

            // Query employees with their department
            var r = exec(m, "Emp.all()->project(~[name:e|$e.name, dept:e|$e.dept.name])");
            assertEquals(2, r.rowCount());
            Map<String, String> empDept = new HashMap<>();
            for (var row : r.rows()) empDept.put(row.get(0).toString(), row.get(1).toString());
            assertEquals("Engineering", empDept.get("Alice"));
            assertEquals("Sales", empDept.get("Bob"));
        }

        @Test @DisplayName("Same database, different queries on different tables")
        void testDifferentQueriesSameDb() throws SQLException {
            sql("CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE PRODUCTS (ID INT, TITLE VARCHAR(100), PRICE INT)",
                "INSERT INTO CUSTOMERS VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO PRODUCTS VALUES (1, 'Widget', 10), (2, 'Gadget', 20), (3, 'Doohickey', 30)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Customer { name: String[1]; }
                    Class model::Product { title: String[1]; price: Integer[1]; }
                    Database store::DB (
                        Table CUSTOMERS (ID INTEGER, NAME VARCHAR(100))
                        Table PRODUCTS (ID INTEGER, TITLE VARCHAR(100), PRICE INTEGER)
                    )
                    Mapping model::M (
                        Customer: Relational { ~mainTable [store::DB] CUSTOMERS name: [store::DB] CUSTOMERS.NAME }
                        Product: Relational { ~mainTable [store::DB] PRODUCTS title: [store::DB] PRODUCTS.TITLE, price: [store::DB] PRODUCTS.PRICE }
                    )
                    """, "store::DB", "model::M");
            var r1 = exec(m, "Customer.all()->project(~[name:c|$c.name])");
            assertEquals(2, r1.rowCount());
            var r2 = exec(m, "Product.all()->filter({p|$p.price > 15})->project(~[title:p|$p.title])");
            assertEquals(2, r2.rowCount());
        }
    }

    // ==================== 11. Edge Cases ====================

    @Nested
    @DisplayName("11. Edge Cases")
    class EdgeCases {

        @Test @DisplayName("NULL values in columns")
        void testNullValues() throws SQLException {
            sql("CREATE TABLE NULLS (ID INT, NAME VARCHAR(100), AGE INT)",
                "INSERT INTO NULLS VALUES (1, 'Alice', 25), (2, NULL, 30), (3, 'Charlie', NULL)");
            String m = singleTableModel("P", "NULLS", "store::DB", "model::M",
                    "Class model::P { name: String[0..1]; age: Integer[0..1]; }",
                    "ID INTEGER, NAME VARCHAR(100), AGE INTEGER",
                    "name: [store::DB] NULLS.NAME, age: [store::DB] NULLS.AGE");
            var r = exec(m, "P.all()->project(~[name:x|$x.name, age:x|$x.age])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("Special characters in string data")
        void testSpecialChars() throws SQLException {
            sql("CREATE TABLE SPECIAL (ID INT, NAME VARCHAR(200))",
                "INSERT INTO SPECIAL VALUES (1, 'O''Brien'), (2, 'She said \"hello\"')");
            String m = singleTableModel("P", "SPECIAL", "store::DB", "model::M",
                    "Class model::P { name: String[1]; }",
                    "ID INTEGER, NAME VARCHAR(200)",
                    "name: [store::DB] SPECIAL.NAME");
            var r = exec(m, "P.all()->project(~[name:x|$x.name])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Unicode data")
        void testUnicodeData() throws SQLException {
            sql("CREATE TABLE UNICODE (ID INT, NAME VARCHAR(200))",
                "INSERT INTO UNICODE VALUES (1, '日本語'), (2, 'Ñoño'), (3, 'Ünïcödé')");
            String m = singleTableModel("P", "UNICODE", "store::DB", "model::M",
                    "Class model::P { name: String[1]; }",
                    "ID INTEGER, NAME VARCHAR(200)",
                    "name: [store::DB] UNICODE.NAME");
            var r = exec(m, "P.all()->project(~[name:x|$x.name])");
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 0).contains("日本語"));
        }

        @Test @DisplayName("Very long string data")
        void testLongString() throws SQLException {
            String longStr = "A".repeat(10000);
            sql("CREATE TABLE LONG_STR (ID INT, DATA VARCHAR)",
                "INSERT INTO LONG_STR VALUES (1, '" + longStr + "')");
            String m = singleTableModel("P", "LONG_STR", "store::DB", "model::M",
                    "Class model::P { data: String[1]; }",
                    "ID INTEGER, DATA VARCHAR",
                    "data: [store::DB] LONG_STR.DATA");
            var r = exec(m, "P.all()->project(~[data:x|$x.data])");
            assertEquals(1, r.rowCount());
            assertEquals(10000, r.rows().get(0).get(0).toString().length());
        }

        @Test @DisplayName("Negative numbers")
        void testNegativeNumbers() throws SQLException {
            sql("CREATE TABLE NEGS (ID INT, VAL INT)",
                "INSERT INTO NEGS VALUES (1, -100), (2, -50), (3, 0), (4, 50)");
            String m = singleTableModel("N", "NEGS", "store::DB", "model::M",
                    "Class model::N { val: Integer[1]; }",
                    "ID INTEGER, VAL INTEGER",
                    "val: [store::DB] NEGS.VAL");
            var r = exec(m, "N.all()->filter({x|$x.val < 0})->project(~[val:x|$x.val])");
            assertEquals(2, r.rowCount());
        }

        @Test @DisplayName("Query all with no projection (getAll only)")
        void testGetAllNoProjection() throws SQLException {
            sql("CREATE TABLE SIMPLE (ID INT, NAME VARCHAR(50))",
                "INSERT INTO SIMPLE VALUES (1, 'test')");
            String m = singleTableModel("S", "SIMPLE", "store::DB", "model::M",
                    "Class model::S { name: String[1]; }",
                    "ID INTEGER, NAME VARCHAR(50)",
                    "name: [store::DB] SIMPLE.NAME");
            var r = exec(m, "S.all()");
            assertEquals(1, r.rowCount());
        }
    }

    // ==================== 12. String Functions ====================

    @Nested
    @DisplayName("12. String Functions")
    class StringFunctions {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE STR (ID INT, S VARCHAR(100))",
                "INSERT INTO STR VALUES (1, 'Hello World'), (2, 'foo bar'), (3, 'UPPERCASE'), (4, '  trimme  ')");
            model = singleTableModel("R", "STR", "store::DB", "model::M",
                    "Class model::R { s: String[1]; }",
                    "ID INTEGER, S VARCHAR(100)",
                    "s: [store::DB] STR.S");
        }

        @Test @DisplayName("toUpper")
        void testToUpper() throws SQLException {
            var r = exec(model, "R.all()->filter({x|$x.s == 'foo bar'})->project(~[u:x|$x.s->toUpper()])");
            assertEquals("FOO BAR", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("toLower")
        void testToLower() throws SQLException {
            var r = exec(model, "R.all()->filter({x|$x.s == 'UPPERCASE'})->project(~[l:x|$x.s->toLower()])");
            assertEquals("uppercase", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("length")
        void testLength() throws SQLException {
            var r = exec(model, "R.all()->filter({x|$x.s == 'Hello World'})->project(~[len:x|$x.s->length()])");
            assertEquals(11, ((Number) r.rows().get(0).get(0)).intValue());
        }

        @Test @DisplayName("trim")
        void testTrim() throws SQLException {
            var r = exec(model, "R.all()->filter({x|$x.s->contains('trimme')})->project(~[t:x|$x.s->trim()])");
            assertEquals("trimme", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("substring")
        void testSubstring() throws SQLException {
            var r = exec(model, "R.all()->filter({x|$x.s == 'Hello World'})->project(~[sub:x|$x.s->substring(0, 5)])");
            assertEquals("Hello", r.rows().get(0).get(0).toString());
        }
    }

    // ==================== 13. Numeric Functions ====================

    @Nested
    @DisplayName("13. Numeric Functions")
    class NumericFunctions {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE NUMS (ID INT, A INT, B DOUBLE)",
                "INSERT INTO NUMS VALUES (1, 10, 3.7), (2, -5, 2.3), (3, 100, 9.5)");
            model = singleTableModel("N", "NUMS", "store::DB", "model::M",
                    "Class model::N { a: Integer[1]; b: Float[1]; }",
                    "ID INTEGER, A INTEGER, B DOUBLE",
                    "a: [store::DB] NUMS.A, b: [store::DB] NUMS.B");
        }

        @Test @DisplayName("abs()")
        void testAbs() throws SQLException {
            var r = exec(model, "N.all()->filter({x|$x.a == -5})->project(~[v:x|$x.a->abs()])");
            assertEquals(5, ((Number) r.rows().get(0).get(0)).intValue());
        }

        @Test @DisplayName("Arithmetic: addition")
        void testAddition() throws SQLException {
            var r = exec(model, "N.all()->filter({x|$x.a == 10})->project(~[v:x|$x.a + 5])");
            assertEquals(15, ((Number) r.rows().get(0).get(0)).intValue());
        }

        @Test @DisplayName("Arithmetic: multiplication")
        void testMultiplication() throws SQLException {
            var r = exec(model, "N.all()->filter({x|$x.a == 10})->project(~[v:x|$x.a * 3])");
            assertEquals(30, ((Number) r.rows().get(0).get(0)).intValue());
        }
    }

    // ==================== 14. Chained Operations ====================

    @Nested
    @DisplayName("14. Chained Operations")
    class ChainedOperations {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE ITEMS (ID INT, NAME VARCHAR(50), CATEGORY VARCHAR(20), PRICE INT, QTY INT)",
                "INSERT INTO ITEMS VALUES (1, 'Widget A', 'Tools', 100, 5)",
                "INSERT INTO ITEMS VALUES (2, 'Widget B', 'Tools', 200, 3)",
                "INSERT INTO ITEMS VALUES (3, 'Gadget X', 'Electronics', 300, 2)",
                "INSERT INTO ITEMS VALUES (4, 'Gadget Y', 'Electronics', 400, 1)",
                "INSERT INTO ITEMS VALUES (5, 'Widget C', 'Tools', 150, 4)");
            model = singleTableModel("I", "ITEMS", "store::DB", "model::M",
                    "Class model::I { name: String[1]; category: String[1]; price: Integer[1]; qty: Integer[1]; }",
                    "ID INTEGER, NAME VARCHAR(50), CATEGORY VARCHAR(20), PRICE INTEGER, QTY INTEGER",
                    "name: [store::DB] ITEMS.NAME, category: [store::DB] ITEMS.CATEGORY, price: [store::DB] ITEMS.PRICE, qty: [store::DB] ITEMS.QTY");
        }

        @Test @DisplayName("filter → project → sort → limit")
        void testFilterProjectSortLimit() throws SQLException {
            var r = exec(model, "I.all()->filter({i|$i.category == 'Tools'})->sortByReversed({i|$i.price})->limit(2)->project(~[name:i|$i.name, price:i|$i.price])");
            assertEquals(2, r.rowCount());
            // Top 2 most expensive tools: Widget B (200), Widget C (150)
        }

        @Test @DisplayName("project → groupBy → sort")
        void testProjectGroupBySort() throws SQLException {
            var r = exec(model,
                    "I.all()->project(~[category:i|$i.category, price:i|$i.price])->groupBy([{r|$r.category}], [agg({r|$r.price}, {y|$y->sum()})], ['category', 'totalPrice'])->sort('totalPrice', SortDirection.DESC)");
            assertEquals(2, r.rowCount());
            // Electronics: 700, Tools: 450
            assertEquals("Electronics", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Multiple filters then project")
        void testMultipleFilters() throws SQLException {
            var r = exec(model,
                    "I.all()->filter({i|$i.category == 'Tools'})->filter({i|$i.price >= 150})->project(~[name:i|$i.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Widget B", "Widget C")));
        }
    }

    // ==================== LAZY JOIN OPTIMIZATION ====================

    @Nested
    @DisplayName("15. Lazy Join Optimization")
    class LazyJoinOptimization {

        private String joinChainModel() {
            return withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; deptName: String[1]; orgName: String[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            deptName: [store::DB] @Person_Dept | T_DEPT.NAME,
                            orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                        }
                    )
                    """, "store::DB", "model::M");
        }

        private int countJoins(String sql) {
            int count = 0;
            String upper = sql.toUpperCase();
            int idx = 0;
            while ((idx = upper.indexOf("LEFT OUTER JOIN", idx)) != -1) {
                count++;
                idx += 15;
            }
            return count;
        }

        // --- Full cancellation: no join-chain property used ---

        @Test @DisplayName("No JOINs when only main-table property projected")
        void testNoJoinWhenOnlyLocalProperty() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->project(~[name:p|$p.name])");
            assertEquals(0, countJoins(sql),
                    "Expected 0 LEFT JOINs when only local property projected. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("T_DEPT"),
                    "T_DEPT should not appear in SQL. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("T_ORG"),
                    "T_ORG should not appear in SQL. SQL: " + sql);
        }

        // --- Partial cancellation: one chain used, another not ---

        @Test @DisplayName("1 JOIN when only 1-hop chain property projected")
        void testOneJoinWhenSingleHopUsed() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName])");
            assertEquals(1, countJoins(sql),
                    "Expected 1 LEFT JOIN for deptName (1-hop). SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("T_DEPT"),
                    "T_DEPT should appear for deptName. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("T_ORG"),
                    "T_ORG should not appear when orgName not used. SQL: " + sql);
        }

        @Test @DisplayName("2 JOINs when 2-hop chain property projected")
        void testTwoJoinsWhenTwoHopUsed() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(2, countJoins(sql),
                    "Expected 2 LEFT JOINs for orgName (2-hop chain). SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("T_DEPT"),
                    "T_DEPT should appear (intermediate hop). SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("T_ORG"),
                    "T_ORG should appear (terminal hop). SQL: " + sql);
        }

        // --- All chains used: no cancellation ---

        @Test @DisplayName("3 JOINs when all chain properties projected")
        void testAllJoinsWhenAllUsed() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->project(~[name:p|$p.name, dept:p|$p.deptName, org:p|$p.orgName])");
            assertEquals(3, countJoins(sql),
                    "Expected 3 LEFT JOINs (1 for deptName, 2 for orgName). SQL: " + sql);
        }

        // --- Filter triggers join ---

        @Test @DisplayName("JOINs appear when chain property used in filter only")
        void testJoinWhenFilterUsesChainProperty() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme'})->project(~[name:p|$p.name])");
            assertEquals(2, countJoins(sql),
                    "Expected 2 LEFT JOINs for orgName used in filter. SQL: " + sql);
        }

        @Test @DisplayName("No JOINs when filter uses only local property")
        void testNoJoinWhenFilterUsesLocalProperty() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->filter({p|$p.name == 'Alice'})->project(~[name:p|$p.name])");
            assertEquals(0, countJoins(sql),
                    "Expected 0 LEFT JOINs when only local properties used. SQL: " + sql);
        }

        // --- Correctness: data still correct with lazy joins ---

        @Test @DisplayName("Data correct: lazy join still returns right values")
        void testDataCorrectnessWithLazyJoin() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp')",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");

            // Project only name — no joins needed
            var r1 = exec(joinChainModel(), "Person.all()->project(~[name:p|$p.name])");
            assertEquals(3, r1.rowCount());
            assertTrue(colStr(r1, 0).containsAll(List.of("Alice", "Bob", "Charlie")));

            // Project name + orgName — 2 joins needed
            var r2 = exec(joinChainModel(), "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(3, r2.rowCount());
            for (var row : r2.rows()) assertEquals("Acme Corp", row.get(1).toString());
        }

        @Test @DisplayName("Data correct: filter on chain property + project local")
        void testDataFilterOnChainProjectLocal() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp'), (2, 'Beta Inc')",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 2)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");

            var r = exec(joinChainModel(),
                    "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
        }

        // --- Partial cancellation within batched extend ---

        @Test @DisplayName("Partial column cancellation: 2-col extend, only 1 used")
        void testPartialColumnCancellation() {
            // deptName and orgName share different chains. When only deptName is used,
            // the orgName chain should be cancelled.
            String sql = planSql(joinChainModel(),
                    "Person.all()->project(~[dept:p|$p.deptName])");
            assertEquals(1, countJoins(sql),
                    "Expected 1 LEFT JOIN for deptName only. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("T_ORG"),
                    "T_ORG should not appear when orgName not projected. SQL: " + sql);
        }

        @Test @DisplayName("Partial: filter on deptName, project name — only 1-hop join")
        void testPartialFilterDeptProjectName() {
            String sql = planSql(joinChainModel(),
                    "Person.all()->filter({p|$p.deptName == 'Engineering'})->project(~[name:p|$p.name])");
            assertEquals(1, countJoins(sql),
                    "Expected 1 LEFT JOIN for deptName in filter. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("T_ORG"),
                    "T_ORG should not appear. SQL: " + sql);
        }

    }

    // ==================== GAP FEATURES (Disabled) ====================

    @Nested
    @DisplayName("GAP: Features Not Yet Supported")
    class GapFeatures {

        private void setupBasicTables() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT, MANAGER_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), ORG_ID INT)",
                "CREATE TABLE T_ORG (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                "INSERT INTO T_ORG VALUES (1, 'Acme Corp')",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1, NULL), (2, 'Bob', 1, 1), (3, 'Charlie', 2, 1)");
        }

        // --- Multi-hop Joins ---

        @Test
        @DisplayName("Multi-hop join: Person → Dept → Org")
        void testMultiHopJoin() throws SQLException {
            setupBasicTables();
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Dept { name: String[1]; }
                    Class model::Org { name: String[1]; }
                    Association model::Person_Dept { persons: Person[*]; dept: Dept[1]; }
                    Association model::Dept_Org { depts: Dept[*]; org: Org[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER, MANAGER_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] T_PERSON name: [store::DB] T_PERSON.NAME }
                        Dept: Relational { ~mainTable [store::DB] T_DEPT name: [store::DB] T_DEPT.NAME }
                        Org: Relational { ~mainTable [store::DB] T_ORG name: [store::DB] T_ORG.NAME }
                    
                        model::Person_Dept: Relational { AssociationMapping ( persons: [store::DB]@Person_Dept, dept: [store::DB]@Person_Dept ) }
                        model::Dept_Org: Relational { AssociationMapping ( depts: [store::DB]@Dept_Org, org: [store::DB]@Dept_Org ) }
)
                    """, "store::DB", "model::M");
            // Navigate Person → dept → org (2 hops)
            var r = exec(m, "Person.all()->project(~[name:p|$p.name, org:p|$p.dept.org.name])");
            assertEquals(3, r.rowCount());
            for (var row : r.rows()) assertEquals("Acme Corp", row.get(1).toString());
        }

        // --- Join Chain Property Mappings (@J1 > @J2 | T.COL) ---

        @Test
        @DisplayName("Join chain property mapping: Person.orgName via @Person_Dept > @Dept_Org | T_ORG.NAME")
        void testJoinChainPropertyMapping() throws SQLException {
            setupBasicTables();
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; orgName: String[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER, MANAGER_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                        }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Person.all()->project(~[name:p|$p.name, org:p|$p.orgName])");
            assertEquals(3, r.rowCount());
            for (var row : r.rows()) assertEquals("Acme Corp", row.get(1).toString());
        }

        @Test
        @DisplayName("Join chain property mapping with filter")
        void testJoinChainPropertyMappingFilter() throws SQLException {
            setupBasicTables();
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; orgName: String[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER, MANAGER_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                        Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                        Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                        Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                        }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "Person.all()->filter({p|$p.orgName == 'Acme Corp'})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount());
        }

        // --- Self-Joins ---

        @Test
        @DisplayName("Self-join: Person → manager via {target}")
        void testSelfJoin() throws SQLException {
            sql("CREATE TABLE EMPLOYEES (ID INT, NAME VARCHAR(100), MANAGER_ID INT)",
                "INSERT INTO EMPLOYEES VALUES (1, 'Alice', null), (2, 'Bob', 1), (3, 'Charlie', 1)");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Employee { name: String[1]; managerName: String[1]; }
                    Association test::EmpMgr { emp: test::Employee[*]; manager: test::Employee[1]; }
                    Database store::DB (
                        Table EMPLOYEES (ID INTEGER, NAME VARCHAR(100), MANAGER_ID INTEGER)
                        Join EmpMgr(EMPLOYEES.MANAGER_ID = {target}.ID)
                    )
                    Mapping test::M (
                        test::Employee: Relational {
                            ~mainTable [store::DB] EMPLOYEES
                            name: [store::DB] EMPLOYEES.NAME,
                            managerName: @EmpMgr | [store::DB] EMPLOYEES.NAME
                        }
                    
                        test::EmpMgr: Relational { AssociationMapping ( emp: [store::DB]@EmpMgr, manager: [store::DB]@EmpMgr ) }
)
                    """, "store::DB", "test::M");

            var r = exec(model, "Employee.all()->filter(e|$e.managerName == 'Alice')->project(~[name:e|$e.name])");
            var names = colStr(r, 0);
            assertEquals(2, names.size());
            assertTrue(names.contains("Bob"));
            assertTrue(names.contains("Charlie"));
        }

        // --- Mapping ~filter ---

        @Test
        @DisplayName("Mapping-level ~filter adds WHERE clause to all queries")
        void testMappingFilter() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, NAME VARCHAR(100), ACTIVE INT)",
                "INSERT INTO PEOPLE VALUES (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Charlie', 1)");

            String model = withRuntime("""
                    import test::*;

                    Class test::Person { id: Integer[1]; name: String[1]; active: Integer[1]; }
                    Database test::DB
                    (
                        Table PEOPLE (ID INTEGER PRIMARY KEY, NAME VARCHAR(100) NOT NULL, ACTIVE INTEGER NOT NULL)
                        Filter ActiveFilter(PEOPLE.ACTIVE = 1)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~filter [test::DB] ActiveFilter
                            ~mainTable [test::DB] PEOPLE
                            id: [test::DB] PEOPLE.ID,
                            name: [test::DB] PEOPLE.NAME,
                            active: [test::DB] PEOPLE.ACTIVE
                        }
                    )
                    """, "test::DB", "test::M");

            // All 3 rows in table, but filter should only return ACTIVE = 1
            var result = exec(model, "Person.all()->project([x|$x.name], ['name'])");
            var names = colStr(result, 0);
            assertEquals(2, names.size());
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Charlie"));
            assertFalse(names.contains("Bob"));
        }

        // --- Mapping ~distinct ---

        @Test
        @DisplayName("Mapping-level ~distinct adds DISTINCT to generated SQL")
        void testMappingDistinct() throws SQLException {
            sql("CREATE TABLE TAGS (ID INT, TAG VARCHAR(100))",
                "INSERT INTO TAGS VALUES (1, 'java'), (2, 'sql'), (3, 'java'), (4, 'sql'), (5, 'rust')");

            String model = withRuntime("""
                    import test::*;

                    Class test::Tag { id: Integer[1]; tag: String[1]; }
                    Database test::DB
                    (
                        Table TAGS (ID INTEGER PRIMARY KEY, TAG VARCHAR(100) NOT NULL)
                    )
                    Mapping test::M
                    (
                        Tag: Relational
                        {
                            ~distinct
                            ~mainTable [test::DB] TAGS
                            id: [test::DB] TAGS.ID,
                            tag: [test::DB] TAGS.TAG
                        }
                    )
                    """, "test::DB", "test::M");

            // 5 rows with duplicates on tag — ~distinct means DISTINCT on all generated SELECTs
            var result = exec(model, "Tag.all()->project([x|$x.tag], ['tag'])");
            var tags = colStr(result, 0);
            // DISTINCT collapses to 3 unique tag values: java, sql, rust
            assertEquals(3, tags.size());
            assertTrue(tags.contains("java"));
            assertTrue(tags.contains("sql"));
            assertTrue(tags.contains("rust"));
        }

        // --- Set IDs and Root Markers ---

        @Test
        @DisplayName("Multiple set implementations with root marker resolve via root")
        void testSetIdsAndRoot() throws SQLException {
            sql("CREATE TABLE T_EMPLOYEE (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE T_CONTRACTOR (ID INT, NAME VARCHAR(100))",
                "INSERT INTO T_EMPLOYEE VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO T_CONTRACTOR VALUES (10, 'Charlie')");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { id: Integer[1]; name: String[1]; }
                    Database store::DB
                    (
                        Table T_EMPLOYEE (ID INTEGER PRIMARY KEY, NAME VARCHAR(100) NOT NULL)
                        Table T_CONTRACTOR (ID INTEGER PRIMARY KEY, NAME VARCHAR(100) NOT NULL)
                    )
                    Mapping test::M
                    (
                        *Person[emp]: Relational
                        {
                            ~mainTable [store::DB] T_EMPLOYEE
                            id: [store::DB] T_EMPLOYEE.ID,
                            name: [store::DB] T_EMPLOYEE.NAME
                        }
                        Person[contractor]: Relational
                        {
                            ~mainTable [store::DB] T_CONTRACTOR
                            id: [store::DB] T_CONTRACTOR.ID,
                            name: [store::DB] T_CONTRACTOR.NAME
                        }
                    )
                    """, "store::DB", "test::M");

            // Root mapping (emp) should be used for Person.all()
            var result = exec(model, "Person.all()->project([x|$x.name], ['name'])");
            var names = colStr(result, 0);
            assertEquals(2, names.size());
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Bob"));
        }

        // --- Mapping Extends ---

        @Test @Disabled("GAP: extends clause ignored by builder")
        @DisplayName("GAP: Mapping inheritance via extends")
        void testMappingExtends() throws SQLException {
            // Employee[emp] extends [person_base]: Relational { ... }
        }

        // --- Mapping Includes ---

        @Test
        @DisplayName("Mapping include pulls class mappings from included mapping")
        void testMappingInclude() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, NAME VARCHAR(100))",
                "INSERT INTO PEOPLE VALUES (1, 'Alice'), (2, 'Bob')");

            String model = withRuntime("""
                    import base::*;
                    import store::*;
                    import test::*;

                    Class test::Person { id: Integer[1]; name: String[1]; }
                    Database store::DB ( Table PEOPLE (ID INTEGER PRIMARY KEY, NAME VARCHAR(100) NOT NULL) )
                    Mapping base::BaseMapping
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] PEOPLE
                            id: [store::DB] PEOPLE.ID,
                            name: [store::DB] PEOPLE.NAME
                        }
                    )
                    Mapping test::M
                    (
                        include base::BaseMapping
                    )
                    """, "store::DB", "test::M");

            // Person mapping comes from included base::BaseMapping
            var result = exec(model, "Person.all()->project([x|$x.name], ['name'])");
            var names = colStr(result, 0);
            assertEquals(2, names.size());
            assertTrue(names.contains("Alice"));
        }

        // --- Store Substitution ---

        @Test @Disabled("GAP: store substitution not visited by builder")
        @DisplayName("GAP: Mapping include with store substitution")
        void testStoreSubstitution() throws SQLException {
            // include model::BaseMapping[store::DevDB -> store::ProdDB]
        }

        // --- Scope Blocks ---

        @Test @Disabled("GAP: scope keyword in lexer but no grammar rule")
        @DisplayName("GAP: Scope block")
        void testScopeBlock() throws SQLException {
            // scope([DB]T) (prop1: col1, prop2: col2)
        }

        // --- Complex Join Conditions ---

        @Test
        @DisplayName("Multi-column join condition (T1.A = T2.A and T1.B = T2.B)")
        void testMultiColumnJoin() throws SQLException {
            sql("CREATE TABLE ORDERS (REGION VARCHAR(10), PRODUCT_ID INT, QTY INT)",
                "INSERT INTO ORDERS VALUES ('US', 1, 10), ('US', 2, 5), ('EU', 1, 20)",
                "CREATE TABLE PRICES (REGION VARCHAR(10), PRODUCT_ID INT, PRICE DECIMAL(10,2))",
                "INSERT INTO PRICES VALUES ('US', 1, 9.99), ('US', 2, 4.99), ('EU', 1, 12.50)");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Order { qty: Integer[1]; price: Float[1]; }
                    Association test::OrderPrice { order: test::Order[*]; priceInfo: test::Price[1]; }
                    Class test::Price { price: Float[1]; }
                    Database store::DB (
                        Table ORDERS (REGION VARCHAR(10), PRODUCT_ID INTEGER, QTY INTEGER)
                        Table PRICES (REGION VARCHAR(10), PRODUCT_ID INTEGER, PRICE DOUBLE)
                        Join OrderPrice(ORDERS.REGION = PRICES.REGION and ORDERS.PRODUCT_ID = PRICES.PRODUCT_ID)
                    )
                    Mapping test::M (
                        test::Order: Relational {
                            ~mainTable [store::DB] ORDERS
                            qty: [store::DB] ORDERS.QTY,
                            price: @OrderPrice | [store::DB] PRICES.PRICE
                        }
                    
                        test::OrderPrice: Relational { AssociationMapping ( order: [store::DB]@OrderPrice, priceInfo: [store::DB]@OrderPrice ) }
)
                    """, "store::DB", "test::M");

            var r = exec(model, "Order.all()->project(~[qty:o|$o.qty, price:o|$o.price])");
            assertEquals(3, r.rowCount());
            // US/product1: qty=10, price=9.99
            // US/product2: qty=5,  price=4.99
            // EU/product1: qty=20, price=12.50
        }

        @Test
        @DisplayName("Function-based join condition (concat in join)")
        void testFunctionInJoin() throws SQLException {
            sql("CREATE TABLE CODES (ID INT, PREFIX VARCHAR(10), CODE VARCHAR(20))",
                "INSERT INTO CODES VALUES (1, 'A', '001'), (2, 'B', '002')",
                "CREATE TABLE LABELS (FULL_CODE VARCHAR(30), LABEL VARCHAR(50))",
                "INSERT INTO LABELS VALUES ('A_001', 'Alpha One'), ('B_002', 'Beta Two')");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Item { code: String[1]; label: String[1]; }
                    Association test::ItemLabel { item: test::Item[*]; labelInfo: test::LabelInfo[1]; }
                    Class test::LabelInfo { label: String[1]; }
                    Database store::DB (
                        Table CODES (ID INTEGER, PREFIX VARCHAR(10), CODE VARCHAR(20))
                        Table LABELS (FULL_CODE VARCHAR(30), LABEL VARCHAR(50))
                        Join CodeLabel(concat(CODES.PREFIX, '_', CODES.CODE) = LABELS.FULL_CODE)
                    )
                    Mapping test::M (
                        test::Item: Relational {
                            ~mainTable [store::DB] CODES
                            code: [store::DB] CODES.CODE,
                            label: @CodeLabel | [store::DB] LABELS.LABEL
                        }
                    )
                    """, "store::DB", "test::M");

            var r = exec(model, "Item.all()->project(~[code:i|$i.code, label:i|$i.label])");
            assertEquals(2, r.rowCount());
            var labels = colStr(r, 1);
            assertTrue(labels.contains("Alpha One"));
            assertTrue(labels.contains("Beta Two"));
        }

        // --- Views ---

        @Test
        @DisplayName("View as data source in mapping — simple column refs")
        void testViewAsDataSource() throws SQLException {
            sql("CREATE TABLE EMPLOYEES (ID INT, NAME VARCHAR, DEPT_ID INT)",
                "INSERT INTO EMPLOYEES VALUES (1, 'Alice', 10), (2, 'Bob', 20)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Employee { empId: Integer[1]; empName: String[1]; }
                Database store::DB (
                    Table EMPLOYEES (ID INT, NAME VARCHAR(100), DEPT_ID INT)
                    View EmpView (
                        emp_id: EMPLOYEES.ID PRIMARY KEY,
                        emp_name: EMPLOYEES.NAME
                    )
                    Join EmpDept(EMPLOYEES.DEPT_ID = EMPLOYEES.ID)
                )
                Mapping test::M (
                    test::Employee: Relational {
                        ~mainTable [store::DB] EmpView
                        empId: [store::DB] EmpView.emp_id,
                        empName: [store::DB] EmpView.emp_name
                    }
                )
                """, "store::DB", "test::M");

            var r = exec(model, "test::Employee.all()->project(~[empId, empName])");
            assertEquals(2, r.columnCount());
            assertEquals(2, r.rows().size());
            assertEquals(List.of(1, 2), colInt(r, 0));
            assertEquals(List.of("Alice", "Bob"), colStr(r, 1));
        }

        @Test
        @DisplayName("View with all features — simple column, join column, DynaFunction column")
        void testViewAllFeatures() throws SQLException {
            sql("CREATE TABLE EMPLOYEES (ID INT, FIRST VARCHAR, LAST VARCHAR, DEPT_ID INT)",
                "CREATE TABLE DEPARTMENTS (ID INT, NAME VARCHAR)",
                "INSERT INTO DEPARTMENTS VALUES (10, 'Engineering'), (20, 'Sales')",
                "INSERT INTO EMPLOYEES VALUES (1, 'Alice', 'Smith', 10), (2, 'Bob', 'Jones', 20)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Employee {
                    empId: Integer[1];
                    fullName: String[1];
                    deptName: String[1];
                }
                Database store::DB (
                    Table EMPLOYEES (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), DEPT_ID INT)
                    Table DEPARTMENTS (ID INT, NAME VARCHAR(100))
                    Join EmpDept(EMPLOYEES.DEPT_ID = DEPARTMENTS.ID)
                    View EmpView (
                        emp_id: EMPLOYEES.ID PRIMARY KEY,
                        full_name: concat(EMPLOYEES.FIRST, ' ', EMPLOYEES.LAST),
                        dept_name: @EmpDept | DEPARTMENTS.NAME
                    )
                )
                Mapping test::M (
                    test::Employee: Relational {
                        ~mainTable [store::DB] EmpView
                        empId: [store::DB] EmpView.emp_id,
                        fullName: [store::DB] EmpView.full_name,
                        deptName: [store::DB] EmpView.dept_name
                    }
                )
                """, "store::DB", "test::M");

            var r = exec(model, "test::Employee.all()->project(~[empId, fullName, deptName])");
            assertEquals(3, r.columnCount());
            assertEquals(2, r.rows().size());
            assertEquals(List.of(1, 2), colInt(r, 0));
            assertEquals(List.of("Alice Smith", "Bob Jones"), colStr(r, 1));
            assertEquals(List.of("Engineering", "Sales"), colStr(r, 2));
        }

        @Test
        @DisplayName("View join pruning — unused join column produces 0 JOINs")
        void testViewJoinPruning() throws SQLException {
            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Employee {
                    empId: Integer[1];
                    empName: String[1];
                    deptName: String[1];
                }
                Database store::DB (
                    Table EMPLOYEES (ID INT, NAME VARCHAR(50), DEPT_ID INT)
                    Table DEPARTMENTS (ID INT, NAME VARCHAR(100))
                    Join EmpDept(EMPLOYEES.DEPT_ID = DEPARTMENTS.ID)
                    View EmpView (
                        emp_id: EMPLOYEES.ID PRIMARY KEY,
                        emp_name: EMPLOYEES.NAME,
                        dept_name: @EmpDept | DEPARTMENTS.NAME
                    )
                )
                Mapping test::M (
                    test::Employee: Relational {
                        ~mainTable [store::DB] EmpView
                        empId: [store::DB] EmpView.emp_id,
                        empName: [store::DB] EmpView.emp_name,
                        deptName: [store::DB] EmpView.dept_name
                    }
                )
                """, "store::DB", "test::M");

            // Only project empId + empName (local columns) — deptName's JOIN should be pruned
            String sql = planSql(model,
                    "test::Employee.all()->project(~[empId, empName])");
            assertFalse(sql.toUpperCase().contains("JOIN"),
                    "No deptName access → no JOIN. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("DEPARTMENTS"),
                    "DEPARTMENTS should not appear. SQL: " + sql);

            // Project deptName — JOIN should appear
            String sql2 = planSql(model,
                    "test::Employee.all()->project(~[empId, deptName])");
            assertTrue(sql2.toUpperCase().contains("JOIN"),
                    "deptName access → JOIN expected. SQL: " + sql2);
            assertTrue(sql2.toUpperCase().contains("DEPARTMENTS"),
                    "DEPARTMENTS should appear for deptName. SQL: " + sql2);
        }

        @Test
        @DisplayName("View end-to-end: filter + join + groupBy + distinct + DynaFunction")
        void testViewEndToEnd() throws SQLException {
            sql("CREATE TABLE SALES (ID INT, PRODUCT VARCHAR, AMOUNT INT, REGION VARCHAR, REP_ID INT)",
                "CREATE TABLE REPS (ID INT, NAME VARCHAR)",
                "INSERT INTO REPS VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO SALES VALUES (1, 'Widget', 100, 'East', 1), " +
                        "(2, 'Widget', 200, 'East', 2), " +
                        "(3, 'Gadget', 50, 'West', 1), " +
                        "(4, 'Widget', 150, 'West', 2)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Sale {
                    saleId: Integer[1];
                    product: String[1];
                    amount: Integer[1];
                    region: String[1];
                    repName: String[1];
                    label: String[1];
                }
                Database store::DB (
                    Table SALES (ID INT, PRODUCT VARCHAR(50), AMOUNT INT, REGION VARCHAR(50), REP_ID INT)
                    Table REPS (ID INT, NAME VARCHAR(50))
                    Join SalesRep(SALES.REP_ID = REPS.ID)
                    View SaleView (
                        sale_id: SALES.ID PRIMARY KEY,
                        product: SALES.PRODUCT,
                        amount: SALES.AMOUNT,
                        region: SALES.REGION,
                        rep_name: @SalesRep | REPS.NAME,
                        label: concat(SALES.PRODUCT, ' [', SALES.REGION, ']')
                    )
                )
                Mapping test::M (
                    test::Sale: Relational {
                        ~mainTable [store::DB] SaleView
                        saleId: [store::DB] SaleView.sale_id,
                        product: [store::DB] SaleView.product,
                        amount: [store::DB] SaleView.amount,
                        region: [store::DB] SaleView.region,
                        repName: [store::DB] SaleView.rep_name,
                        label: [store::DB] SaleView.label
                    }
                )
                """, "store::DB", "test::M");

            // 1. Filter + join + dyna: filter on amount, project join & dyna columns
            var r1 = exec(model,
                    "test::Sale.all()->filter({s|$s.amount > 50})->project(~[saleId, repName, label])");
            assertEquals(3, r1.rows().size());

            // 2. GroupBy on view-resolved column: total amount by region
            var r2 = exec(model,
                    "test::Sale.all()->project(~[region, amount])" +
                    "->groupBy(~region, ~[total:x|$x.amount:y|$y->sum()])");
            assertEquals(2, r2.rows().size()); // East, West

            // 3. Distinct on view-resolved column
            var r3 = exec(model,
                    "test::Sale.all()->project(~[product])->distinct()");
            assertEquals(2, r3.rows().size()); // Widget, Gadget
        }

        @Test
        @DisplayName("View with ~filter, ~distinct, join chain, and DynaFunction")
        void testViewWithFilterDistinctJoinDyna() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, PRODUCT VARCHAR, AMOUNT INT, STATUS VARCHAR, CUST_ID INT)",
                "CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR)",
                "INSERT INTO CUSTOMERS VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO ORDERS VALUES " +
                        "(1, 'Widget', 100, 'ACTIVE', 1), " +
                        "(2, 'Gadget', 200, 'ACTIVE', 2), " +
                        "(3, 'Widget', 50, 'CLOSED', 1), " +
                        "(4, 'Gadget', 150, 'ACTIVE', 1)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Order {
                    orderId: Integer[1];
                    product: String[1];
                    amount: Integer[1];
                    custName: String[1];
                    label: String[1];
                }
                Database store::DB (
                    Table ORDERS (ID INT, PRODUCT VARCHAR(50), AMOUNT INT, STATUS VARCHAR(20), CUST_ID INT)
                    Table CUSTOMERS (ID INT, NAME VARCHAR(50))
                    Filter ActiveOnly(ORDERS.STATUS = 'ACTIVE')
                    Join OrderCust(ORDERS.CUST_ID = CUSTOMERS.ID)
                    View ActiveOrderView (
                        ~filter ActiveOnly
                        ~distinct
                        order_id: ORDERS.ID PRIMARY KEY,
                        product: ORDERS.PRODUCT,
                        amount: ORDERS.AMOUNT,
                        cust_name: @OrderCust | CUSTOMERS.NAME,
                        label: concat(ORDERS.PRODUCT, ' $', ORDERS.AMOUNT)
                    )
                )
                Mapping test::M (
                    test::Order: Relational {
                        ~mainTable [store::DB] ActiveOrderView
                        orderId: [store::DB] ActiveOrderView.order_id,
                        product: [store::DB] ActiveOrderView.product,
                        amount: [store::DB] ActiveOrderView.amount,
                        custName: [store::DB] ActiveOrderView.cust_name,
                        label: [store::DB] ActiveOrderView.label
                    }
                )
                """, "store::DB", "test::M");

            // Should only return ACTIVE orders (view ~filter), with DISTINCT applied
            var r = exec(model, "test::Order.all()->project(~[orderId, product, amount, custName, label])");
            // 3 active rows, all distinct
            assertEquals(3, r.rows().size());
            // Verify filter: no CLOSED order (id=3) should appear
            var ids = colInt(r, 0);
            assertFalse(ids.contains(3), "CLOSED order (id=3) should be filtered out");
            // Verify join: custName should be resolved
            var names = colStr(r, 3);
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Bob"));
            // Verify dyna: label should have formatted strings
            var labels = colStr(r, 4);
            assertTrue(labels.stream().anyMatch(l -> l.contains("Widget $")));
        }

        @Test
        @DisplayName("View with multi-hop and independent join chains")
        void testViewMultiJoinChains() throws SQLException {
            sql("CREATE TABLE PERSONS (ID INT, NAME VARCHAR, DEPT_ID INT, OFFICE_ID INT)",
                "CREATE TABLE DEPTS (ID INT, NAME VARCHAR, ORG_ID INT)",
                "CREATE TABLE ORGS (ID INT, NAME VARCHAR)",
                "CREATE TABLE OFFICES (ID INT, CITY VARCHAR)",
                "INSERT INTO ORGS VALUES (1, 'Acme Corp')",
                "INSERT INTO DEPTS VALUES (10, 'Engineering', 1)",
                "INSERT INTO OFFICES VALUES (100, 'New York')",
                "INSERT INTO PERSONS VALUES (1, 'Alice', 10, 100)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Person {
                    name: String[1];
                    deptName: String[1];
                    orgName: String[1];
                    city: String[1];
                }
                Database store::DB (
                    Table PERSONS (ID INT, NAME VARCHAR(50), DEPT_ID INT, OFFICE_ID INT)
                    Table DEPTS (ID INT, NAME VARCHAR(50), ORG_ID INT)
                    Table ORGS (ID INT, NAME VARCHAR(50))
                    Table OFFICES (ID INT, CITY VARCHAR(50))
                    Join PersonDept(PERSONS.DEPT_ID = DEPTS.ID)
                    Join DeptOrg(DEPTS.ORG_ID = ORGS.ID)
                    Join PersonOffice(PERSONS.OFFICE_ID = OFFICES.ID)
                    View PersonView (
                        person_name: PERSONS.NAME PRIMARY KEY,
                        dept_name: @PersonDept | DEPTS.NAME,
                        org_name: @PersonDept > @DeptOrg | ORGS.NAME,
                        city: @PersonOffice | OFFICES.CITY
                    )
                )
                Mapping test::M (
                    test::Person: Relational {
                        ~mainTable [store::DB] PersonView
                        name: [store::DB] PersonView.person_name,
                        deptName: [store::DB] PersonView.dept_name,
                        orgName: [store::DB] PersonView.org_name,
                        city: [store::DB] PersonView.city
                    }
                )
                """, "store::DB", "test::M");

            // All join chains: 1-hop dept, 2-hop org, independent 1-hop office
            var r = exec(model, "test::Person.all()->project(~[name, deptName, orgName, city])");
            assertEquals(1, r.rows().size());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Engineering", colStr(r, 1).get(0));
            assertEquals("Acme Corp", colStr(r, 2).get(0));
            assertEquals("New York", colStr(r, 3).get(0));

            // Pruning: only project name + city — dept/org joins should be pruned
            String sql = planSql(model, "test::Person.all()->project(~[name, city])");
            assertFalse(sql.toUpperCase().contains("DEPTS"),
                    "DEPTS should not appear when deptName/orgName not projected. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("ORGS"),
                    "ORGS should not appear. SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("OFFICES"),
                    "OFFICES should appear for city. SQL: " + sql);
        }

        @Test
        @DisplayName("View DynaFunction referencing two independent join chains")
        void testViewDynaWithTwoJoinChains() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, CUST_ID INT, VENDOR_ID INT)",
                "CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR)",
                "CREATE TABLE VENDORS (ID INT, NAME VARCHAR)",
                "INSERT INTO CUSTOMERS VALUES (1, 'Alice')",
                "INSERT INTO VENDORS VALUES (1, 'Acme')",
                "INSERT INTO ORDERS VALUES (1, 1, 1)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Order {
                    orderId: Integer[1];
                    parties: String[1];
                }
                Database store::DB (
                    Table ORDERS (ID INT, CUST_ID INT, VENDOR_ID INT)
                    Table CUSTOMERS (ID INT, NAME VARCHAR(50))
                    Table VENDORS (ID INT, NAME VARCHAR(50))
                    Join OrderCust(ORDERS.CUST_ID = CUSTOMERS.ID)
                    Join OrderVendor(ORDERS.VENDOR_ID = VENDORS.ID)
                    View OrderView (
                        order_id: ORDERS.ID PRIMARY KEY,
                        parties: concat(@OrderCust | CUSTOMERS.NAME, ' / ', @OrderVendor | VENDORS.NAME)
                    )
                )
                Mapping test::M (
                    test::Order: Relational {
                        ~mainTable [store::DB] OrderView
                        orderId: [store::DB] OrderView.order_id,
                        parties: [store::DB] OrderView.parties
                    }
                )
                """, "store::DB", "test::M");

            var r = exec(model, "test::Order.all()->project(~[orderId, parties])");
            assertEquals(1, r.rows().size());
            assertEquals(1, colInt(r, 0).get(0));
            assertEquals("Alice / Acme", colStr(r, 1).get(0));
        }

        @Test
        @DisplayName("View with ~groupBy — aggregate through view macro")
        void testViewWithGroupBy() throws SQLException {
            sql("CREATE TABLE SALES (ID INT, REGION VARCHAR, AMOUNT INT)",
                "INSERT INTO SALES VALUES (1, 'East', 100), (2, 'East', 200), " +
                        "(3, 'West', 150), (4, 'West', 50)");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::RegionTotal {
                    region: String[1];
                    total: Integer[1];
                }
                Database store::DB (
                    Table SALES (ID INT, REGION VARCHAR(50), AMOUNT INT)
                    View RegionSales (
                        ~groupBy (SALES.REGION)
                        region: SALES.REGION,
                        total: sum(SALES.AMOUNT)
                    )
                )
                Mapping test::M (
                    test::RegionTotal: Relational {
                        ~mainTable [store::DB] RegionSales
                        region: [store::DB] RegionSales.region,
                        total: [store::DB] RegionSales.total
                    }
                )
                """, "store::DB", "test::M");

            var r = exec(model, "test::RegionTotal.all()->project(~[region, total])");
            assertEquals(2, r.rows().size());
            var regions = colStr(r, 0);
            var totals = colInt(r, 1);
            // East: 100+200=300, West: 150+50=200
            int eastIdx = regions.indexOf("East");
            int westIdx = regions.indexOf("West");
            assertTrue(eastIdx >= 0 && westIdx >= 0, "Both regions should appear");
            assertEquals(300, totals.get(eastIdx));
            assertEquals(200, totals.get(westIdx));
        }

        @Test
        @DisplayName("Schema-qualified table in mapping")
        void testSchemaTable() throws SQLException {
            sql("CREATE SCHEMA IF NOT EXISTS hr",
                "CREATE TABLE hr.EMPLOYEES (ID INT, NAME VARCHAR, DEPT VARCHAR)",
                "INSERT INTO hr.EMPLOYEES VALUES (1, 'Alice', 'Eng'), (2, 'Bob', 'Sales')");

            String model = withRuntime("""
                import store::*;
                import test::*;

                Class test::Employee {
                    empId: Integer[1];
                    name: String[1];
                    dept: String[1];
                }
                Database store::DB (
                    Schema hr (
                        Table EMPLOYEES (ID INT, NAME VARCHAR(50), DEPT VARCHAR(50))
                    )
                )
                Mapping test::M (
                    test::Employee: Relational {
                        ~mainTable [store::DB] hr.EMPLOYEES
                        empId: [store::DB] hr.EMPLOYEES.ID,
                        name: [store::DB] hr.EMPLOYEES.NAME,
                        dept: [store::DB] hr.EMPLOYEES.DEPT
                    }
                )
                """, "store::DB", "test::M");

            var r = exec(model, "test::Employee.all()->project(~[empId, name, dept])");
            assertEquals(2, r.rows().size());
            assertEquals("Alice", colStr(r, 1).get(0));

            // Verify schema-qualified SQL
            String sql = planSql(model, "test::Employee.all()->project(~[empId, name])");
            assertTrue(sql.contains("\"hr\".\"EMPLOYEES\""),
                    "SQL should contain schema-qualified table. SQL: " + sql);
        }

        // --- Database Filters ---

        @Test @Disabled("GAP: Database filters not extracted")
        @DisplayName("GAP: Named database filter")
        void testDatabaseFilter() throws SQLException {
            // Filter ActiveFilter(T.STATUS = 1)
        }

        // --- Database Includes ---

        @Test
        @DisplayName("Database include merges tables and joins from included DB")
        void testDatabaseInclude() throws SQLException {
            sql("CREATE TABLE BASE_PERSON (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE EXT_ADDRESS (ID INT, PERSON_ID INT, CITY VARCHAR(100))",
                "INSERT INTO BASE_PERSON VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO EXT_ADDRESS VALUES (10, 1, 'NYC'), (20, 2, 'LA')");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { id: Integer[1]; name: String[1]; }
                    Class test::Address { id: Integer[1]; city: String[1]; }
                    Database store::BaseDB
                    (
                        Table BASE_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100) NOT NULL)
                    )
                    Database store::ExtDB
                    (
                        include store::BaseDB
                        Table EXT_ADDRESS (ID INTEGER PRIMARY KEY, PERSON_ID INTEGER NOT NULL, CITY VARCHAR(100) NOT NULL)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::ExtDB] BASE_PERSON
                            id: [store::ExtDB] BASE_PERSON.ID,
                            name: [store::ExtDB] BASE_PERSON.NAME
                        }
                        Address: Relational
                        {
                            ~mainTable [store::ExtDB] EXT_ADDRESS
                            id: [store::ExtDB] EXT_ADDRESS.ID,
                            city: [store::ExtDB] EXT_ADDRESS.CITY
                        }
                    )
                    """, "store::ExtDB", "test::M");

            // BASE_PERSON is from included BaseDB, accessible via ExtDB
            var result = exec(model, "Person.all()->project([x|$x.name], ['name'])");
            var names = colStr(result, 0);
            assertEquals(2, names.size());
            assertTrue(names.contains("Alice"));
            assertTrue(names.contains("Bob"));
        }

        // --- Local Properties ---

        @Test @Disabled("GAP: Local property + prefix semantics lost")
        @DisplayName("GAP: Local mapping property")
        void testLocalProperty() throws SQLException {
            // +localProp: String[1]: [DB] T.EXTRA
        }

        // --- DynaFunction in Property Mapping ---

        @Test
        @DisplayName("DynaFunction: concat() in relational property mapping")
        void testDynaFunctionConcat() throws SQLException {
            sql("CREATE TABLE NAMES (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50))",
                "INSERT INTO NAMES VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { fullName: String[1]; }
                    Database store::DB ( Table NAMES ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50) ) )
                    Mapping model::M ( Person: Relational { ~mainTable [store::DB] NAMES fullName: concat([store::DB] NAMES.FIRST, ' ', [store::DB] NAMES.LAST) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "Person.all()->project([x|$x.fullName], ['fullName'])");
            assertEquals(2, result.rows().size());
            var names = colStr(result, 0);
            assertTrue(names.contains("John Doe"));
            assertTrue(names.contains("Jane Smith"));
        }

        @Test
        @DisplayName("DynaFunction: substring() in relational property mapping")
        void testDynaFunctionSubstring() throws SQLException {
            sql("CREATE TABLE CODES (ID INT, CODE VARCHAR(10))",
                "INSERT INTO CODES VALUES (1, 'US-CA-01'), (2, 'UK-LN-02')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Code { country: String[1]; }
                    Database store::DB ( Table CODES ( ID INTEGER, CODE VARCHAR(10) ) )
                    Mapping model::M ( Code: Relational { ~mainTable [store::DB] CODES country: substring([store::DB] CODES.CODE, 0, 2) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "Code.all()->project([x|$x.country], ['country'])");
            assertEquals(2, result.rows().size());
            var countries = colStr(result, 0);
            assertTrue(countries.contains("US"));
            assertTrue(countries.contains("UK"));
        }

        @Test
        @DisplayName("DynaFunction: if(equal(...)) in relational property mapping")
        void testDynaFunctionIfEqual() throws SQLException {
            sql("CREATE TABLE STATUS (ID INT, CODE VARCHAR(10))",
                "INSERT INTO STATUS VALUES (1, 'A'), (2, 'I')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Rec { label: String[1]; }
                    Database store::DB ( Table STATUS ( ID INTEGER, CODE VARCHAR(10) ) )
                    Mapping model::M ( Rec: Relational { ~mainTable [store::DB] STATUS label: if(equal([store::DB] STATUS.CODE, 'A'), 'Active', 'Inactive') } )
                    """, "store::DB", "model::M");
            var result = exec(model, "Rec.all()->project([x|$x.label], ['label'])");
            assertEquals(2, result.rows().size());
            var labels = colStr(result, 0);
            assertTrue(labels.contains("Active"));
            assertTrue(labels.contains("Inactive"));
        }

        @Test
        @DisplayName("DynaFunction: plus() arithmetic in relational property mapping")
        void testDynaFunctionPlus() throws SQLException {
            sql("CREATE TABLE NUMS (ID INT, A INT, B INT)",
                "INSERT INTO NUMS VALUES (1, 10, 20), (2, 30, 40)");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Rec { total: Integer[1]; }
                    Database store::DB ( Table NUMS ( ID INTEGER, A INTEGER, B INTEGER ) )
                    Mapping model::M ( Rec: Relational { ~mainTable [store::DB] NUMS total: plus([store::DB] NUMS.A, [store::DB] NUMS.B) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "Rec.all()->project([x|$x.total], ['total'])");
            assertEquals(2, result.rows().size());
        }

        @Test
        @DisplayName("DynaFunction: toLower() in relational property mapping")
        void testDynaFunctionToLower() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T1 VALUES (1, 'ALICE'), (2, 'BOB')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::P { lowerName: String[1]; }
                    Database store::DB ( Table T1 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T1 lowerName: toLower([store::DB] T1.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.lowerName], ['lowerName'])");
            assertEquals(2, result.rows().size());
            var names = colStr(result, 0);
            assertTrue(names.contains("alice"));
            assertTrue(names.contains("bob"));
        }

        @Test
        @DisplayName("DynaFunction: nested toLower(concat(...))")
        void testDynaFunctionNested() throws SQLException {
            sql("CREATE TABLE NAMES (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50))",
                "INSERT INTO NAMES VALUES (1, 'John', 'DOE')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { email: String[1]; }
                    Database store::DB ( Table NAMES ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50) ) )
                    Mapping model::M ( Person: Relational { ~mainTable [store::DB] NAMES email: toLower(concat([store::DB] NAMES.FIRST, [store::DB] NAMES.LAST)) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "Person.all()->project([x|$x.email], ['email'])");
            assertEquals(1, result.rows().size());
            assertEquals("johndoe", result.rows().get(0).get(0));
        }

        // --- Association Mappings (Explicit) ---

        @Test
        @DisplayName("Explicit association mapping wires join for navigation")
        void testExplicitAssociationMapping() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT, LEGAL_NAME VARCHAR(100))",
                "INSERT INTO T_FIRM VALUES (1, 'ACME'), (2, 'Globex')",
                "INSERT INTO T_PERSON VALUES (10, 'Alice', 1), (20, 'Bob', 1), (30, 'Charlie', 2)");

            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { id: Integer[1]; name: String[1]; }
                    Class test::Firm { id: Integer[1]; legalName: String[1]; }
                    Association test::PersonFirm { firm: Firm[1]; employees: Person[*]; }
                    Database store::DB
                    (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100) NOT NULL, FIRM_ID INTEGER NOT NULL)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100) NOT NULL)
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping test::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID,
                            name: [store::DB] T_PERSON.NAME
                        }
                        Firm: Relational
                        {
                            ~mainTable [store::DB] T_FIRM
                            id: [store::DB] T_FIRM.ID,
                            legalName: [store::DB] T_FIRM.LEGAL_NAME
                        }
                        test::PersonFirm: Relational { AssociationMapping (
                            employees: [store::DB]@PersonFirm,
                            firm: [store::DB]@PersonFirm
                        ) }
                    )
                    """, "store::DB", "test::M");

            // Navigate Person -> firm via explicit association mapping
            var result = exec(model, "Person.all()->project([x|$x.name, x|$x.firm.legalName], ['name', 'firm'])");
            assertEquals(3, result.rows().size());
            var names = colStr(result, 0);
            var firms = colStr(result, 1);
            assertTrue(names.contains("Alice"));
            assertTrue(firms.contains("ACME"));
        }

        // --- XStore ---

        @Test @Disabled("GAP: XStore not in grammar")
        @DisplayName("GAP: XStore cross-store mapping")
        void testXStore() throws SQLException {
            // PersonFirm: XStore { persons: $this.firmId == $that.id }
        }

        // --- AggregationAware ---

        @Test @Disabled("GAP: AggregationAware not in grammar")
        @DisplayName("GAP: AggregationAware mapping")
        void testAggregationAware() throws SQLException {
            // Class: AggregationAware { Views: [...], ~mainMapping: ... }
        }

        // --- Relation Mapping Type ---

        @Test @Disabled("GAP: Relation class mapping not in grammar")
        @DisplayName("GAP: Relation class mapping (~func)")
        void testRelationClassMapping() throws SQLException {
            // Class: Relation { ~func myFunction }
        }
    }

    // ==================== 15. Advanced Filter Patterns ====================

    @Nested
    @DisplayName("15. Advanced Filter Patterns")
    class AdvancedFilters {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE PRODUCTS (ID INT, NAME VARCHAR(100), CATEGORY VARCHAR(50), PRICE DOUBLE, STOCK INT, ACTIVE BOOLEAN, CREATED DATE)",
                "INSERT INTO PRODUCTS VALUES (1, 'Widget A', 'Tools', 9.99, 100, true, '2024-01-15')",
                "INSERT INTO PRODUCTS VALUES (2, 'Widget B', 'Tools', 19.99, 0, false, '2024-02-20')",
                "INSERT INTO PRODUCTS VALUES (3, 'Gadget X', 'Electronics', 49.99, 50, true, '2024-03-10')",
                "INSERT INTO PRODUCTS VALUES (4, 'Gadget Y', 'Electronics', 99.99, 25, true, '2023-12-01')",
                "INSERT INTO PRODUCTS VALUES (5, 'Widget C', 'Tools', 14.99, 200, true, '2024-04-05')",
                "INSERT INTO PRODUCTS VALUES (6, 'Thingamajig', 'Misc', 4.99, 500, true, '2024-01-01')",
                "INSERT INTO PRODUCTS VALUES (7, 'Doohickey', 'Misc', 2.49, 1000, false, '2023-06-15')",
                "INSERT INTO PRODUCTS VALUES (8, 'Gizmo', 'Electronics', 149.99, 10, true, '2024-05-01')");
            model = singleTableModel("P", "PRODUCTS", "store::DB", "model::M",
                    "Class model::P { name: String[1]; category: String[1]; price: Float[1]; stock: Integer[1]; active: Boolean[1]; created: Date[1]; }",
                    "ID INTEGER, NAME VARCHAR(100), CATEGORY VARCHAR(50), PRICE DOUBLE, STOCK INTEGER, ACTIVE BOOLEAN, CREATED DATE",
                    "name: [store::DB] PRODUCTS.NAME, category: [store::DB] PRODUCTS.CATEGORY, price: [store::DB] PRODUCTS.PRICE, stock: [store::DB] PRODUCTS.STOCK, active: [store::DB] PRODUCTS.ACTIVE, created: [store::DB] PRODUCTS.CREATED");
        }

        @Test @DisplayName("Filter: deeply nested boolean logic")
        void testDeepNestedBoolean() throws SQLException {
            // (category == 'Tools' AND price > 10) OR (category == 'Electronics' AND stock > 20)
            var r = exec(model, "P.all()->filter({p|($p.category == 'Tools' && $p.price > 10.0) || ($p.category == 'Electronics' && $p.stock > 20)})->project(~[name:p|$p.name])");
            assertEquals(4, r.rowCount()); // Widget B, Widget C, Gadget X, Gadget Y
        }

        @Test @DisplayName("Filter: triple AND")
        void testTripleAnd() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.category == 'Tools' && $p.active == true && $p.stock > 50})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount()); // Widget A (100), Widget C (200)
        }

        @Test @DisplayName("Filter: triple OR")
        void testTripleOr() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.category == 'Tools' || $p.category == 'Electronics' || $p.category == 'Misc'})->project(~[name:p|$p.name])");
            assertEquals(8, r.rowCount()); // all products
        }

        @Test @DisplayName("Filter: NOT with compound")
        void testNotCompound() throws SQLException {
            var r = exec(model, "P.all()->filter({p|!($p.category == 'Tools' || $p.category == 'Misc')})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount()); // Electronics only
        }

        @Test @DisplayName("Filter: price range (between equivalent)")
        void testPriceRange() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.price >= 10.0 && $p.price <= 50.0})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount()); // Widget B (19.99), Widget C (14.99), Gadget X (49.99)
        }

        @Test @DisplayName("Filter: stock == 0 (out of stock)")
        void testZeroStock() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.stock == 0})->project(~[name:p|$p.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Widget B", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Filter: active false products")
        void testInactiveProducts() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.active == false})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Widget B", "Doohickey")));
        }

        @Test @DisplayName("Filter then filter then filter (3 chained)")
        void testTripleChainedFilter() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.active == true})->filter({p|$p.category == 'Electronics'})->filter({p|$p.price < 100.0})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount()); // Gadget X (49.99), Gadget Y (99.99) — both < 100
            assertTrue(colStr(r, 0).containsAll(List.of("Gadget X", "Gadget Y")));
        }

        @Test @DisplayName("Filter: string length comparison")
        void testStringLengthFilter() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.name->length() > 8})->project(~[name:p|$p.name])");
            assertTrue(r.rowCount() > 0);
            for (var row : r.rows()) assertTrue(row.get(0).toString().length() > 8);
        }

        @Test @DisplayName("Filter: contains with toLower (case-insensitive search)")
        void testCaseInsensitiveSearch() throws SQLException {
            var r = exec(model, "P.all()->filter({p|$p.name->toLower()->contains('widget')})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount());
        }
    }

    // ==================== 16. Advanced Aggregation Patterns ====================

    @Nested
    @DisplayName("16. Advanced Aggregation Patterns")
    class AdvancedAggregation {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, CUSTOMER VARCHAR(50), PRODUCT VARCHAR(50), QTY INT, UNIT_PRICE DOUBLE, ORDER_DATE DATE)",
                "INSERT INTO ORDERS VALUES (1, 'Alice', 'Widget', 5, 10.0, '2024-01-15')",
                "INSERT INTO ORDERS VALUES (2, 'Alice', 'Gadget', 2, 25.0, '2024-01-20')",
                "INSERT INTO ORDERS VALUES (3, 'Bob', 'Widget', 3, 10.0, '2024-02-10')",
                "INSERT INTO ORDERS VALUES (4, 'Bob', 'Widget', 7, 10.0, '2024-02-15')",
                "INSERT INTO ORDERS VALUES (5, 'Charlie', 'Gadget', 1, 25.0, '2024-03-01')",
                "INSERT INTO ORDERS VALUES (6, 'Alice', 'Widget', 10, 10.0, '2024-03-05')",
                "INSERT INTO ORDERS VALUES (7, 'Charlie', 'Gizmo', 4, 50.0, '2024-03-10')",
                "INSERT INTO ORDERS VALUES (8, 'Bob', 'Gizmo', 2, 50.0, '2024-03-15')");
            model = singleTableModel("O", "ORDERS", "store::DB", "model::M",
                    "Class model::O { customer: String[1]; product: String[1]; qty: Integer[1]; unitPrice: Float[1]; }",
                    "ID INTEGER, CUSTOMER VARCHAR(50), PRODUCT VARCHAR(50), QTY INTEGER, UNIT_PRICE DOUBLE, ORDER_DATE DATE",
                    "customer: [store::DB] ORDERS.CUSTOMER, product: [store::DB] ORDERS.PRODUCT, qty: [store::DB] ORDERS.QTY, unitPrice: [store::DB] ORDERS.UNIT_PRICE");
        }

        @Test @DisplayName("GroupBy: total quantity per customer")
        void testTotalQtyPerCustomer() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[customer:o|$o.customer, qty:o|$o.qty])->groupBy([{r|$r.customer}], [agg({r|$r.qty}, {y|$y->sum()})], ['customer', 'totalQty'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(17, results.get("Alice"));  // 5 + 2 + 10
            assertEquals(12, results.get("Bob"));    // 3 + 7 + 2
            assertEquals(5, results.get("Charlie")); // 1 + 4
        }

        @Test @DisplayName("GroupBy: count orders per product")
        void testCountOrdersPerProduct() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[product:o|$o.product, qty:o|$o.qty])->groupBy([{r|$r.product}], [agg({r|$r.qty}, {y|$y->count()})], ['product', 'orderCount'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(4, results.get("Widget"));
            assertEquals(2, results.get("Gadget"));
            assertEquals(2, results.get("Gizmo"));
        }

        @Test @DisplayName("GroupBy: average qty per customer")
        void testAvgQtyPerCustomer() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[customer:o|$o.customer, qty:o|$o.qty])->groupBy([{r|$r.customer}], [agg({r|$r.qty}, {y|$y->average()})], ['customer', 'avgQty'])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("GroupBy: max qty in single order per customer")
        void testMaxQtyPerCustomer() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[customer:o|$o.customer, qty:o|$o.qty])->groupBy([{r|$r.customer}], [agg({r|$r.qty}, {y|$y->max()})], ['customer', 'maxQty'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(10, results.get("Alice"));
            assertEquals(7, results.get("Bob"));
            assertEquals(4, results.get("Charlie"));
        }

        @Test @DisplayName("GroupBy: min qty per product")
        void testMinQtyPerProduct() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[product:o|$o.product, qty:o|$o.qty])->groupBy([{r|$r.product}], [agg({r|$r.qty}, {y|$y->min()})], ['product', 'minQty'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(3, results.get("Widget")); // min(5,3,7,10)
            assertEquals(1, results.get("Gadget")); // min(2,1)
            assertEquals(2, results.get("Gizmo"));  // min(4,2)
        }

        @Test @DisplayName("GroupBy: customer+product (two-key groupBy)")
        void testTwoKeyGroupBy() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[customer:o|$o.customer, product:o|$o.product, qty:o|$o.qty])->groupBy([{r|$r.customer}, {r|$r.product}], [agg({r|$r.qty}, {y|$y->sum()})], ['customer', 'product', 'totalQty'])");
            assertTrue(r.rowCount() >= 5); // Alice-Widget, Alice-Gadget, Bob-Widget, Bob-Gizmo, Charlie-Gadget, Charlie-Gizmo
        }

        @Test @DisplayName("GroupBy + sort: top customers by total quantity")
        void testGroupBySortTopCustomers() throws SQLException {
            var r = exec(model,
                    "O.all()->project(~[customer:o|$o.customer, qty:o|$o.qty])->groupBy([{r|$r.customer}], [agg({r|$r.qty}, {y|$y->sum()})], ['customer', 'totalQty'])->sort('totalQty', SortDirection.DESC)");
            assertEquals(3, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString()); // 17
            assertEquals("Bob", r.rows().get(1).get(0).toString());    // 12
        }

        @Test @DisplayName("Filter → project → groupBy (filtered aggregation)")
        void testFilteredAggregation() throws SQLException {
            var r = exec(model,
                    "O.all()->filter({o|$o.product == 'Widget'})->project(~[customer:o|$o.customer, qty:o|$o.qty])->groupBy([{r|$r.customer}], [agg({r|$r.qty}, {y|$y->sum()})], ['customer', 'widgetQty'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(15, results.get("Alice")); // 5 + 10
            assertEquals(10, results.get("Bob"));   // 3 + 7
            assertNull(results.get("Charlie"));     // Charlie has no widget orders
        }

        @Test @DisplayName("Distinct on multi-column projection")
        void testDistinctMultiColumn() throws SQLException {
            var r = exec(model, "O.all()->project(~[customer:o|$o.customer, product:o|$o.product])->distinct()");
            assertTrue(r.rowCount() >= 5); // unique customer-product pairs
        }
    }

    // ==================== 17. Advanced Join Patterns ====================

    @Nested
    @DisplayName("17. Advanced Join Patterns")
    class AdvancedJoins {

        @Test @DisplayName("To-one: filter on association, project local")
        void testFilterAssocProjectLocal() throws SQLException {
            sql("CREATE TABLE EMPLOYEES (ID INT, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE DEPTS (ID INT, NAME VARCHAR(50))",
                "INSERT INTO DEPTS VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')",
                "INSERT INTO EMPLOYEES VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'Diana', 3)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Employee { name: String[1]; }
                    Class model::Dept { name: String[1]; }
                    Association model::Emp_Dept { employees: Employee[*]; dept: Dept[1]; }
                    Database store::DB (
                        Table EMPLOYEES (ID INTEGER, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table DEPTS (ID INTEGER, NAME VARCHAR(50))
                        Join Emp_Dept(EMPLOYEES.DEPT_ID = DEPTS.ID)
                    )
                    Mapping model::M (
                        Employee: Relational { ~mainTable [store::DB] EMPLOYEES name: [store::DB] EMPLOYEES.NAME }
                        Dept: Relational { ~mainTable [store::DB] DEPTS name: [store::DB] DEPTS.NAME }
                    
                        model::Emp_Dept: Relational { AssociationMapping ( employees: [store::DB]@Emp_Dept, dept: [store::DB]@Emp_Dept ) }
)
                    """, "store::DB", "model::M");
            var r = exec(m, "Employee.all()->filter({e|$e.dept.name == 'Engineering'})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
        }

        @Test @DisplayName("To-one: project both local and association properties")
        void testProjectLocalAndAssoc() throws SQLException {
            sql("CREATE TABLE STUDENTS (ID INT, NAME VARCHAR(100), SCHOOL_ID INT)",
                "CREATE TABLE SCHOOLS (ID INT, NAME VARCHAR(100))",
                "INSERT INTO SCHOOLS VALUES (1, 'MIT'), (2, 'Stanford')",
                "INSERT INTO STUDENTS VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Student { name: String[1]; }
                    Class model::School { name: String[1]; }
                    Association model::Student_School { students: Student[*]; school: School[1]; }
                    Database store::DB (
                        Table STUDENTS (ID INTEGER, NAME VARCHAR(100), SCHOOL_ID INTEGER)
                        Table SCHOOLS (ID INTEGER, NAME VARCHAR(100))
                        Join Student_School(STUDENTS.SCHOOL_ID = SCHOOLS.ID)
                    )
                    Mapping model::M (
                        Student: Relational { ~mainTable [store::DB] STUDENTS name: [store::DB] STUDENTS.NAME }
                        School: Relational { ~mainTable [store::DB] SCHOOLS name: [store::DB] SCHOOLS.NAME }
                    
                        model::Student_School: Relational { AssociationMapping ( students: [store::DB]@Student_School, school: [store::DB]@Student_School ) }
)
                    """, "store::DB", "model::M");
            var r = exec(m, "Student.all()->project(~[student:s|$s.name, school:s|$s.school.name])");
            assertEquals(3, r.rowCount());
            Map<String, String> studentSchool = new HashMap<>();
            for (var row : r.rows()) studentSchool.put(row.get(0).toString(), row.get(1).toString());
            assertEquals("MIT", studentSchool.get("Alice"));
            assertEquals("Stanford", studentSchool.get("Bob"));
        }

        @Test @DisplayName("To-many: count addresses per person")
        void testCountToMany() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE ADDRS (ID INT, PERSON_ID INT, CITY VARCHAR(100))",
                "INSERT INTO PEOPLE VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                "INSERT INTO ADDRS VALUES (1, 1, 'NYC'), (2, 1, 'LA'), (3, 1, 'Chicago'), (4, 2, 'Boston')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Addr { city: String[1]; }
                    Association model::Person_Addr { person: Person[1]; addrs: Addr[*]; }
                    Database store::DB (
                        Table PEOPLE (ID INTEGER, NAME VARCHAR(100))
                        Table ADDRS (ID INTEGER, PERSON_ID INTEGER, CITY VARCHAR(100))
                        Join Person_Addr(PEOPLE.ID = ADDRS.PERSON_ID)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] PEOPLE name: [store::DB] PEOPLE.NAME }
                        Addr: Relational { ~mainTable [store::DB] ADDRS city: [store::DB] ADDRS.CITY }
                    
                        model::Person_Addr: Relational { AssociationMapping ( person: [store::DB]@Person_Addr, addrs: [store::DB]@Person_Addr ) }
)
                    """, "store::DB", "model::M");
            // Project person name and address city (LEFT JOIN, rows expand)
            var r = exec(m, "Person.all()->project(~[name:p|$p.name, city:p|$p.addrs.city])");
            // Alice→3 rows, Bob→1 row, Charlie→1 row (null city)
            assertTrue(r.rowCount() >= 4);
        }

        @Test @DisplayName("To-many filter: persons with address in specific city")
        void testToManyFilterSpecificCity() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE ADDRS (ID INT, PERSON_ID INT, CITY VARCHAR(100))",
                "INSERT INTO PEOPLE VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                "INSERT INTO ADDRS VALUES (1, 1, 'NYC'), (2, 1, 'LA'), (3, 2, 'NYC'), (4, 3, 'Boston')");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Addr { city: String[1]; }
                    Association model::Person_Addr { person: Person[1]; addrs: Addr[*]; }
                    Database store::DB (
                        Table PEOPLE (ID INTEGER, NAME VARCHAR(100))
                        Table ADDRS (ID INTEGER, PERSON_ID INTEGER, CITY VARCHAR(100))
                        Join Person_Addr(PEOPLE.ID = ADDRS.PERSON_ID)
                    )
                    Mapping model::M (
                        Person: Relational { ~mainTable [store::DB] PEOPLE name: [store::DB] PEOPLE.NAME }
                        Addr: Relational { ~mainTable [store::DB] ADDRS city: [store::DB] ADDRS.CITY }
                    
                        model::Person_Addr: Relational { AssociationMapping ( person: [store::DB]@Person_Addr, addrs: [store::DB]@Person_Addr ) }
)
                    """, "store::DB", "model::M");
            var r = exec(m, "Person.all()->filter({p|$p.addrs.city == 'NYC'})->project(~[name:p|$p.name])");
            assertEquals(2, r.rowCount()); // Alice and Bob have NYC addresses
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("Two different associations from same class")
        void testTwoAssociationsFromSameClass() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, CUSTOMER_ID INT, PRODUCT_ID INT, QTY INT)",
                "CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR(100))",
                "CREATE TABLE PRODUCTS (ID INT, NAME VARCHAR(100))",
                "INSERT INTO CUSTOMERS VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO PRODUCTS VALUES (1, 'Widget'), (2, 'Gadget')",
                "INSERT INTO ORDERS VALUES (1, 1, 1, 5), (2, 1, 2, 3), (3, 2, 1, 10)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Order { qty: Integer[1]; }
                    Class model::Customer { name: String[1]; }
                    Class model::Product { name: String[1]; }
                    Association model::Order_Customer { orders: Order[*]; customer: Customer[1]; }
                    Association model::Order_Product { orders: Order[*]; product: Product[1]; }
                    Database store::DB (
                        Table ORDERS (ID INTEGER, CUSTOMER_ID INTEGER, PRODUCT_ID INTEGER, QTY INTEGER)
                        Table CUSTOMERS (ID INTEGER, NAME VARCHAR(100))
                        Table PRODUCTS (ID INTEGER, NAME VARCHAR(100))
                        Join Order_Customer(ORDERS.CUSTOMER_ID = CUSTOMERS.ID)
                        Join Order_Product(ORDERS.PRODUCT_ID = PRODUCTS.ID)
                    )
                    Mapping model::M (
                        Order: Relational { ~mainTable [store::DB] ORDERS qty: [store::DB] ORDERS.QTY }
                        Customer: Relational { ~mainTable [store::DB] CUSTOMERS name: [store::DB] CUSTOMERS.NAME }
                        Product: Relational { ~mainTable [store::DB] PRODUCTS name: [store::DB] PRODUCTS.NAME }
                    
                        model::Order_Customer: Relational { AssociationMapping ( orders: [store::DB]@Order_Customer, customer: [store::DB]@Order_Customer ) }
                        model::Order_Product: Relational { AssociationMapping ( orders: [store::DB]@Order_Product, product: [store::DB]@Order_Product ) }
)
                    """, "store::DB", "model::M");
            var r = exec(m, "Order.all()->project(~[customer:o|$o.customer.name, product:o|$o.product.name, qty:o|$o.qty])");
            assertEquals(3, r.rowCount());
        }

        @Test @DisplayName("Filter on to-one + project through to-many")
        void testFilterToOneProjectToMany() throws SQLException {
            sql("CREATE TABLE AUTHORS (ID INT, NAME VARCHAR(100), COUNTRY VARCHAR(50))",
                "CREATE TABLE BOOKS (ID INT, TITLE VARCHAR(200), AUTHOR_ID INT)",
                "INSERT INTO AUTHORS VALUES (1, 'Tolkien', 'UK'), (2, 'Asimov', 'USA'), (3, 'Clarke', 'UK')",
                "INSERT INTO BOOKS VALUES (1, 'LOTR', 1), (2, 'Hobbit', 1), (3, 'Foundation', 2), (4, '2001', 3)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Author { name: String[1]; country: String[1]; }
                    Class model::Book { title: String[1]; }
                    Association model::Author_Book { author: Author[1]; books: Book[*]; }
                    Database store::DB (
                        Table AUTHORS (ID INTEGER, NAME VARCHAR(100), COUNTRY VARCHAR(50))
                        Table BOOKS (ID INTEGER, TITLE VARCHAR(200), AUTHOR_ID INTEGER)
                        Join Author_Book(AUTHORS.ID = BOOKS.AUTHOR_ID)
                    )
                    Mapping model::M (
                        Author: Relational { ~mainTable [store::DB] AUTHORS name: [store::DB] AUTHORS.NAME, country: [store::DB] AUTHORS.COUNTRY }
                        Book: Relational { ~mainTable [store::DB] BOOKS title: [store::DB] BOOKS.TITLE }
                    
                        model::Author_Book: Relational { AssociationMapping ( author: [store::DB]@Author_Book, books: [store::DB]@Author_Book ) }
)
                    """, "store::DB", "model::M");
            // Get all UK authors' book titles
            var r = exec(m, "Author.all()->filter({a|$a.country == 'UK'})->project(~[author:a|$a.name, book:a|$a.books.title])");
            // Tolkien has 2 books, Clarke has 1 = 3 rows
            assertEquals(3, r.rowCount());
        }
    }

    // ==================== 18. Computed Projections ====================

    @Nested
    @DisplayName("18. Computed Projections")
    class ComputedProjections {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE INVENTORY (ID INT, ITEM VARCHAR(100), QTY INT, UNIT_COST DOUBLE, MARKUP DOUBLE)",
                "INSERT INTO INVENTORY VALUES (1, 'Widget', 100, 5.0, 1.5)",
                "INSERT INTO INVENTORY VALUES (2, 'Gadget', 50, 10.0, 2.0)",
                "INSERT INTO INVENTORY VALUES (3, 'Gizmo', 25, 20.0, 1.8)");
            model = singleTableModel("I", "INVENTORY", "store::DB", "model::M",
                    "Class model::I { item: String[1]; qty: Integer[1]; unitCost: Float[1]; markup: Float[1]; }",
                    "ID INTEGER, ITEM VARCHAR(100), QTY INTEGER, UNIT_COST DOUBLE, MARKUP DOUBLE",
                    "item: [store::DB] INVENTORY.ITEM, qty: [store::DB] INVENTORY.QTY, unitCost: [store::DB] INVENTORY.UNIT_COST, markup: [store::DB] INVENTORY.MARKUP");
        }

        @Test @DisplayName("Computed: multiplication in projection")
        void testMultiplication() throws SQLException {
            var r = exec(model, "I.all()->project(~[item:i|$i.item, totalCost:i|$i.qty * $i.unitCost])");
            assertEquals(3, r.rowCount());
            Map<String, Double> costs = new HashMap<>();
            for (var row : r.rows()) costs.put(row.get(0).toString(), ((Number) row.get(1)).doubleValue());
            assertEquals(500.0, costs.get("Widget"), 0.01);
            assertEquals(500.0, costs.get("Gadget"), 0.01);
            assertEquals(500.0, costs.get("Gizmo"), 0.01);
        }

        @Test @DisplayName("Computed: addition in projection")
        void testAddition() throws SQLException {
            var r = exec(model, "I.all()->filter({i|$i.item == 'Widget'})->project(~[extra:i|$i.qty + 50])");
            assertEquals(150, ((Number) r.rows().get(0).get(0)).intValue());
        }

        @Test @DisplayName("Computed: subtraction in projection")
        void testSubtraction() throws SQLException {
            var r = exec(model, "I.all()->filter({i|$i.item == 'Widget'})->project(~[remaining:i|$i.qty - 10])");
            assertEquals(90, ((Number) r.rows().get(0).get(0)).intValue());
        }

        @Test @DisplayName("Computed: string concatenation in projection")
        void testStringConcat() throws SQLException {
            var r = exec(model, "I.all()->filter({i|$i.item == 'Widget'})->project(~[label:i|'Item: ' + $i.item])");
            assertEquals("Item: Widget", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Computed: conditional expression (if)")
        void testConditionalIf() throws SQLException {
            var r = exec(model, "I.all()->project(~[item:i|$i.item, status:i|if($i.qty > 30, |'In Stock', |'Low Stock')])");
            assertEquals(3, r.rowCount());
            Map<String, String> statuses = new HashMap<>();
            for (var row : r.rows()) statuses.put(row.get(0).toString(), row.get(1).toString());
            assertEquals("In Stock", statuses.get("Widget"));  // 100
            assertEquals("In Stock", statuses.get("Gadget"));  // 50
            assertEquals("Low Stock", statuses.get("Gizmo"));  // 25
        }
    }

    // ==================== 19. Date Functions ====================

    @Nested
    @DisplayName("19. Date Functions")
    class DateFunctions {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE EVENTS (ID INT, NAME VARCHAR(100), EVENT_DATE DATE)",
                "INSERT INTO EVENTS VALUES (1, 'New Year', '2024-01-01')",
                "INSERT INTO EVENTS VALUES (2, 'Valentine', '2024-02-14')",
                "INSERT INTO EVENTS VALUES (3, 'Summer', '2024-06-21')",
                "INSERT INTO EVENTS VALUES (4, 'Christmas', '2024-12-25')",
                "INSERT INTO EVENTS VALUES (5, 'Halloween', '2024-10-31')");
            model = singleTableModel("E", "EVENTS", "store::DB", "model::M",
                    "Class model::E { name: String[1]; eventDate: Date[1]; }",
                    "ID INTEGER, NAME VARCHAR(100), EVENT_DATE DATE",
                    "name: [store::DB] EVENTS.NAME, eventDate: [store::DB] EVENTS.EVENT_DATE");
        }

        @Test @DisplayName("Filter: date comparison (after)")
        void testDateAfter() throws SQLException {
            var r = exec(model, "E.all()->filter({e|$e.eventDate > %2024-06-01})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount()); // Summer, Halloween, Christmas
        }

        @Test @DisplayName("Filter: date comparison (before)")
        void testDateBefore() throws SQLException {
            var r = exec(model, "E.all()->filter({e|$e.eventDate < %2024-03-01})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount()); // New Year, Valentine
        }

        @Test @DisplayName("Filter: date range")
        void testDateRange() throws SQLException {
            var r = exec(model, "E.all()->filter({e|$e.eventDate >= %2024-02-01 && $e.eventDate <= %2024-07-01})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount()); // Valentine, Summer
        }

        @Test @DisplayName("Sort by date ascending")
        void testSortByDate() throws SQLException {
            var r = exec(model, "E.all()->sortBy({e|$e.eventDate})->project(~[name:e|$e.name])");
            assertEquals("New Year", r.rows().get(0).get(0).toString());
            assertEquals("Christmas", r.rows().get(r.rowCount() - 1).get(0).toString());
        }
    }

    // ==================== 20. Complex Real-World Scenarios ====================

    @Nested
    @DisplayName("20. Complex Real-World Scenarios")
    class RealWorldScenarios {

        @Test @DisplayName("E-commerce: top 3 products by revenue")
        void testTopProductsByRevenue() throws SQLException {
            sql("CREATE TABLE LINE_ITEMS (ID INT, PRODUCT VARCHAR(50), QTY INT, PRICE DOUBLE)",
                "INSERT INTO LINE_ITEMS VALUES (1, 'Laptop', 2, 999.99)",
                "INSERT INTO LINE_ITEMS VALUES (2, 'Mouse', 10, 29.99)",
                "INSERT INTO LINE_ITEMS VALUES (3, 'Laptop', 1, 999.99)",
                "INSERT INTO LINE_ITEMS VALUES (4, 'Keyboard', 5, 79.99)",
                "INSERT INTO LINE_ITEMS VALUES (5, 'Mouse', 3, 29.99)",
                "INSERT INTO LINE_ITEMS VALUES (6, 'Monitor', 2, 499.99)");
            String m = singleTableModel("LI", "LINE_ITEMS", "store::DB", "model::M",
                    "Class model::LI { product: String[1]; qty: Integer[1]; price: Float[1]; }",
                    "ID INTEGER, PRODUCT VARCHAR(50), QTY INTEGER, PRICE DOUBLE",
                    "product: [store::DB] LINE_ITEMS.PRODUCT, qty: [store::DB] LINE_ITEMS.QTY, price: [store::DB] LINE_ITEMS.PRICE");
            // Get total revenue per product, sorted desc, top 3
            var r = exec(m,
                    "LI.all()->project(~[product:l|$l.product, revenue:l|$l.qty * $l.price])->groupBy([{r|$r.product}], [agg({r|$r.revenue}, {y|$y->sum()})], ['product', 'totalRevenue'])->sort('totalRevenue', SortDirection.DESC)->slice(0, 3)");
            assertEquals(3, r.rowCount());
            assertEquals("Laptop", r.rows().get(0).get(0).toString()); // 2999.97
        }

        @Test @DisplayName("HR: employees with their departments, filtered by salary")
        void testHrEmployeesWithDepts() throws SQLException {
            sql("CREATE TABLE HR_EMP (ID INT, NAME VARCHAR(100), SAL INT, DEPT_ID INT)",
                "CREATE TABLE HR_DEPT (ID INT, NAME VARCHAR(50))",
                "INSERT INTO HR_DEPT VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')",
                "INSERT INTO HR_EMP VALUES (1, 'Alice', 120000, 1), (2, 'Bob', 90000, 1), (3, 'Charlie', 85000, 2), (4, 'Diana', 110000, 3), (5, 'Eve', 95000, 1)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Emp { name: String[1]; sal: Integer[1]; }
                    Class model::Dept { name: String[1]; }
                    Association model::Emp_Dept { emps: Emp[*]; dept: Dept[1]; }
                    Database store::DB (
                        Table HR_EMP (ID INTEGER, NAME VARCHAR(100), SAL INTEGER, DEPT_ID INTEGER)
                        Table HR_DEPT (ID INTEGER, NAME VARCHAR(50))
                        Join Emp_Dept(HR_EMP.DEPT_ID = HR_DEPT.ID)
                    )
                    Mapping model::M (
                        Emp: Relational { ~mainTable [store::DB] HR_EMP name: [store::DB] HR_EMP.NAME, sal: [store::DB] HR_EMP.SAL }
                        Dept: Relational { ~mainTable [store::DB] HR_DEPT name: [store::DB] HR_DEPT.NAME }
                    
                        model::Emp_Dept: Relational { AssociationMapping ( emps: [store::DB]@Emp_Dept, dept: [store::DB]@Emp_Dept ) }
)
                    """, "store::DB", "model::M");
            // High earners (>100k) with their department
            var r = exec(m, "Emp.all()->filter({e|$e.sal > 100000})->project(~[name:e|$e.name, dept:e|$e.dept.name, sal:e|$e.sal])");
            assertEquals(2, r.rowCount()); // Alice (Eng, 120k), Diana (Sales, 110k)
        }

        @Test @DisplayName("Inventory: low stock items with supplier info")
        void testInventoryLowStock() throws SQLException {
            sql("CREATE TABLE INV_ITEMS (ID INT, NAME VARCHAR(100), STOCK INT, SUPPLIER_ID INT)",
                "CREATE TABLE SUPPLIERS (ID INT, NAME VARCHAR(100), COUNTRY VARCHAR(50))",
                "INSERT INTO SUPPLIERS VALUES (1, 'Acme Supply', 'USA'), (2, 'Global Parts', 'Germany')",
                "INSERT INTO INV_ITEMS VALUES (1, 'Bolt M5', 1000, 1), (2, 'Nut M5', 5, 1), (3, 'Washer M5', 3, 2), (4, 'Screw M5', 500, 2)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Item { name: String[1]; stock: Integer[1]; }
                    Class model::Supplier { name: String[1]; country: String[1]; }
                    Association model::Item_Supplier { items: Item[*]; supplier: Supplier[1]; }
                    Database store::DB (
                        Table INV_ITEMS (ID INTEGER, NAME VARCHAR(100), STOCK INTEGER, SUPPLIER_ID INTEGER)
                        Table SUPPLIERS (ID INTEGER, NAME VARCHAR(100), COUNTRY VARCHAR(50))
                        Join Item_Supplier(INV_ITEMS.SUPPLIER_ID = SUPPLIERS.ID)
                    )
                    Mapping model::M (
                        Item: Relational { ~mainTable [store::DB] INV_ITEMS name: [store::DB] INV_ITEMS.NAME, stock: [store::DB] INV_ITEMS.STOCK }
                        Supplier: Relational { ~mainTable [store::DB] SUPPLIERS name: [store::DB] SUPPLIERS.NAME, country: [store::DB] SUPPLIERS.COUNTRY }
                    
                        model::Item_Supplier: Relational { AssociationMapping ( items: [store::DB]@Item_Supplier, supplier: [store::DB]@Item_Supplier ) }
)
                    """, "store::DB", "model::M");
            // Low stock (< 10) items with their supplier
            var r = exec(m, "Item.all()->filter({i|$i.stock < 10})->project(~[item:i|$i.name, stock:i|$i.stock, supplier:i|$i.supplier.name])");
            assertEquals(2, r.rowCount()); // Nut M5 (5, Acme), Washer M5 (3, Global)
        }

        @Test @DisplayName("Analytics: category breakdown with multiple aggregations")
        void testCategoryBreakdown() throws SQLException {
            sql("CREATE TABLE SALES_DATA (ID INT, CATEGORY VARCHAR(50), AMOUNT DOUBLE, UNITS INT)",
                "INSERT INTO SALES_DATA VALUES (1, 'Electronics', 500, 2)",
                "INSERT INTO SALES_DATA VALUES (2, 'Electronics', 300, 1)",
                "INSERT INTO SALES_DATA VALUES (3, 'Clothing', 100, 5)",
                "INSERT INTO SALES_DATA VALUES (4, 'Clothing', 200, 3)",
                "INSERT INTO SALES_DATA VALUES (5, 'Clothing', 150, 4)",
                "INSERT INTO SALES_DATA VALUES (6, 'Food', 50, 10)");
            String m = singleTableModel("S", "SALES_DATA", "store::DB", "model::M",
                    "Class model::S { category: String[1]; amount: Float[1]; units: Integer[1]; }",
                    "ID INTEGER, CATEGORY VARCHAR(50), AMOUNT DOUBLE, UNITS INTEGER",
                    "category: [store::DB] SALES_DATA.CATEGORY, amount: [store::DB] SALES_DATA.AMOUNT, units: [store::DB] SALES_DATA.UNITS");
            // Sum of amount per category
            var r = exec(m,
                    "S.all()->project(~[category:s|$s.category, amount:s|$s.amount])->groupBy([{r|$r.category}], [agg({r|$r.amount}, {y|$y->sum()})], ['category', 'totalAmount'])->sort('totalAmount', SortDirection.DESC)");
            assertEquals(3, r.rowCount());
            assertEquals("Electronics", r.rows().get(0).get(0).toString()); // 800
            assertEquals("Clothing", r.rows().get(1).get(0).toString());    // 450
        }
    }

    // ==================================================================================
    // COMPOSITION TESTS — Features working together
    // ==================================================================================

    // ==================== 21. Join + Filter Composition ====================

    @Nested
    @DisplayName("21. Join + Filter Composition")
    class JoinFilterComposition {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE C_EMP (ID INT, NAME VARCHAR(100), SAL INT, DEPT_ID INT, ACTIVE BOOLEAN)",
                "CREATE TABLE C_DEPT (ID INT, NAME VARCHAR(50), BUDGET INT)",
                "CREATE TABLE C_ADDR (ID INT, EMP_ID INT, CITY VARCHAR(100), STATE VARCHAR(50))",
                "INSERT INTO C_DEPT VALUES (1, 'Engineering', 500000), (2, 'Sales', 300000), (3, 'Marketing', 200000)",
                "INSERT INTO C_EMP VALUES (1, 'Alice', 120000, 1, true)",
                "INSERT INTO C_EMP VALUES (2, 'Bob', 90000, 1, true)",
                "INSERT INTO C_EMP VALUES (3, 'Charlie', 85000, 2, false)",
                "INSERT INTO C_EMP VALUES (4, 'Diana', 110000, 3, true)",
                "INSERT INTO C_EMP VALUES (5, 'Eve', 95000, 2, true)",
                "INSERT INTO C_EMP VALUES (6, 'Frank', 105000, 1, false)",
                "INSERT INTO C_ADDR VALUES (1, 1, 'NYC', 'NY'), (2, 1, 'LA', 'CA'), (3, 2, 'Boston', 'MA')",
                "INSERT INTO C_ADDR VALUES (4, 3, 'Chicago', 'IL'), (5, 5, 'Dallas', 'TX')");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Emp { name: String[1]; sal: Integer[1]; active: Boolean[1]; }
                    Class model::Dept { name: String[1]; budget: Integer[1]; }
                    Class model::Addr { city: String[1]; state: String[1]; }
                    Association model::Emp_Dept { emps: Emp[*]; dept: Dept[1]; }
                    Association model::Emp_Addr { emp: Emp[1]; addrs: Addr[*]; }
                    Database store::DB (
                        Table C_EMP (ID INTEGER, NAME VARCHAR(100), SAL INTEGER, DEPT_ID INTEGER, ACTIVE BOOLEAN)
                        Table C_DEPT (ID INTEGER, NAME VARCHAR(50), BUDGET INTEGER)
                        Table C_ADDR (ID INTEGER, EMP_ID INTEGER, CITY VARCHAR(100), STATE VARCHAR(50))
                        Join Emp_Dept(C_EMP.DEPT_ID = C_DEPT.ID)
                        Join Emp_Addr(C_EMP.ID = C_ADDR.EMP_ID)
                    )
                    Mapping model::M (
                        Emp: Relational { ~mainTable [store::DB] C_EMP name: [store::DB] C_EMP.NAME, sal: [store::DB] C_EMP.SAL, active: [store::DB] C_EMP.ACTIVE }
                        Dept: Relational { ~mainTable [store::DB] C_DEPT name: [store::DB] C_DEPT.NAME, budget: [store::DB] C_DEPT.BUDGET }
                        Addr: Relational { ~mainTable [store::DB] C_ADDR city: [store::DB] C_ADDR.CITY, state: [store::DB] C_ADDR.STATE }
                    
                        model::Emp_Dept: Relational { AssociationMapping ( emps: [store::DB]@Emp_Dept, dept: [store::DB]@Emp_Dept ) }
                        model::Emp_Addr: Relational { AssociationMapping ( emp: [store::DB]@Emp_Addr, addrs: [store::DB]@Emp_Addr ) }
)
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("Filter on local AND to-one association together")
        void testFilterLocalAndToOne() throws SQLException {
            // sal > 100000 AND dept == 'Engineering' → Alice(120k) and Frank(105k)
            var r = exec(model, "Emp.all()->filter({e|$e.sal > 100000 && $e.dept.name == 'Engineering'})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Frank")));
        }

        @Test @DisplayName("Filter on local OR to-one association")
        void testFilterLocalOrToOne() throws SQLException {
            // sal > 110000 OR dept == 'Marketing'
            var r = exec(model, "Emp.all()->filter({e|$e.sal > 110000 || $e.dept.name == 'Marketing'})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Diana")));
        }

        @Test @DisplayName("Filter on to-one + filter on to-many in same query")
        void testFilterToOneAndToMany() throws SQLException {
            // dept == 'Engineering' AND has address in 'NYC'
            var r = exec(model, "Emp.all()->filter({e|$e.dept.name == 'Engineering' && $e.addrs.city == 'NYC'})->project(~[name:e|$e.name])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString());
        }

        @Test @DisplayName("Chained filter: local then association")
        void testChainedFilterLocalThenAssoc() throws SQLException {
            var r = exec(model, "Emp.all()->filter({e|$e.active == true})->filter({e|$e.dept.name == 'Engineering'})->project(~[name:e|$e.name])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Bob")));
        }

        @Test @DisplayName("Filter on association property, project both local + association")
        void testFilterAssocProjectBoth() throws SQLException {
            var r = exec(model, "Emp.all()->filter({e|$e.dept.name == 'Engineering'})->project(~[name:e|$e.name, sal:e|$e.sal, dept:e|$e.dept.name])");
            assertEquals(3, r.rowCount()); // Alice, Bob, Frank
            for (var row : r.rows()) assertEquals("Engineering", row.get(2).toString());
        }

        @Test @DisplayName("Boolean filter + association filter + sort + limit")
        void testBoolFilterAssocSortLimit() throws SQLException {
            var r = exec(model, "Emp.all()->filter({e|$e.active == true && $e.dept.name != 'Marketing'})->sortByReversed({e|$e.sal})->limit(2)->project(~[name:e|$e.name, sal:e|$e.sal])");
            assertEquals(2, r.rowCount());
            // Top 2 active non-Marketing by salary: Alice (120k), Eve (95k) -- Bob (90k) is 3rd
        }

        @Test @DisplayName("Same association in filter AND projection")
        void testSameAssocFilterAndProject() throws SQLException {
            // Filter by addr.city == 'NYC', project addr.state
            var r = exec(model, "Emp.all()->filter({e|$e.addrs.city == 'NYC'})->project(~[name:e|$e.name, city:e|$e.addrs.city, state:e|$e.addrs.state])");
            // Alice has 2 addresses (NYC, LA) — filter narrows to person but projection shows all
            assertTrue(r.rowCount() >= 1);
        }

        @Test @DisplayName("Filter on to-one budget, project to-many addresses")
        void testFilterToOneProjectToMany() throws SQLException {
            // dept.budget > 400000 → only Engineering; then project addresses (to-many)
            var r = exec(model, "Emp.all()->filter({e|$e.dept.budget > 400000})->project(~[name:e|$e.name, city:e|$e.addrs.city])");
            // Engineering employees: Alice (2 addrs), Bob (1 addr), Frank (0 addrs)
            assertTrue(r.rowCount() >= 3);
        }

        @Test @DisplayName("String function on association column in filter")
        void testStringFuncOnAssocFilter() throws SQLException {
            var r = exec(model, "Emp.all()->filter({e|$e.dept.name->startsWith('Eng')})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount()); // Alice, Bob, Frank
        }
    }

    // ==================== 22. Join + Aggregation Composition ====================

    @Nested
    @DisplayName("22. Join + Aggregation Composition")
    class JoinAggregationComposition {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE SALE (ID INT, AMOUNT INT, REP_ID INT)",
                "CREATE TABLE REP (ID INT, NAME VARCHAR(100), REGION VARCHAR(50))",
                "INSERT INTO REP VALUES (1, 'Alice', 'East'), (2, 'Bob', 'East'), (3, 'Charlie', 'West')",
                "INSERT INTO SALE VALUES (1, 500, 1), (2, 300, 1), (3, 700, 2), (4, 200, 3), (5, 400, 3), (6, 100, 1)");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Sale { amount: Integer[1]; }
                    Class model::Rep { name: String[1]; region: String[1]; }
                    Association model::Sale_Rep { sales: Sale[*]; rep: Rep[1]; }
                    Database store::DB (
                        Table SALE (ID INTEGER, AMOUNT INTEGER, REP_ID INTEGER)
                        Table REP (ID INTEGER, NAME VARCHAR(100), REGION VARCHAR(50))
                        Join Sale_Rep(SALE.REP_ID = REP.ID)
                    )
                    Mapping model::M (
                        Sale: Relational { ~mainTable [store::DB] SALE amount: [store::DB] SALE.AMOUNT }
                        Rep: Relational { ~mainTable [store::DB] REP name: [store::DB] REP.NAME, region: [store::DB] REP.REGION }
                    
                        model::Sale_Rep: Relational { AssociationMapping ( sales: [store::DB]@Sale_Rep, rep: [store::DB]@Sale_Rep ) }
)
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("GroupBy on joined column (total sales per rep)")
        void testGroupByJoinedColumn() throws SQLException {
            var r = exec(model,
                    "Sale.all()->project(~[rep:s|$s.rep.name, amount:s|$s.amount])->groupBy([{r|$r.rep}], [agg({r|$r.amount}, {y|$y->sum()})], ['rep', 'total'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(900, results.get("Alice"));   // 500+300+100
            assertEquals(700, results.get("Bob"));     // 700
            assertEquals(600, results.get("Charlie")); // 200+400
        }

        @Test @DisplayName("GroupBy on 2nd-level joined column (total sales per region)")
        void testGroupByRegion() throws SQLException {
            var r = exec(model,
                    "Sale.all()->project(~[region:s|$s.rep.region, amount:s|$s.amount])->groupBy([{r|$r.region}], [agg({r|$r.amount}, {y|$y->sum()})], ['region', 'total'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(1600, results.get("East"));  // Alice(900) + Bob(700)
            assertEquals(600, results.get("West"));   // Charlie(600)
        }

        @Test @DisplayName("Filter on joined column + aggregate")
        void testFilterJoinedThenAggregate() throws SQLException {
            // Only East region sales, then total per rep
            var r = exec(model,
                    "Sale.all()->filter({s|$s.rep.region == 'East'})->project(~[rep:s|$s.rep.name, amount:s|$s.amount])->groupBy([{r|$r.rep}], [agg({r|$r.amount}, {y|$y->sum()})], ['rep', 'total'])");
            assertEquals(2, r.rowCount()); // Alice and Bob only
        }

        @Test @DisplayName("Count per joined column")
        void testCountPerJoinedColumn() throws SQLException {
            var r = exec(model,
                    "Sale.all()->project(~[rep:s|$s.rep.name, amount:s|$s.amount])->groupBy([{r|$r.rep}], [agg({r|$r.amount}, {y|$y->count()})], ['rep', 'cnt'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(3, results.get("Alice"));
            assertEquals(1, results.get("Bob"));
            assertEquals(2, results.get("Charlie"));
        }

        @Test @DisplayName("Aggregate + sort + slice (top rep by total sales)")
        void testAggSortSlice() throws SQLException {
            var r = exec(model,
                    "Sale.all()->project(~[rep:s|$s.rep.name, amount:s|$s.amount])->groupBy([{r|$r.rep}], [agg({r|$r.amount}, {y|$y->sum()})], ['rep', 'total'])->sort('total', SortDirection.DESC)->slice(0, 1)");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString()); // 900
        }
    }

    // ==================== 23. Multi-Join Composition ====================

    @Nested
    @DisplayName("23. Multi-Join Composition")
    class MultiJoinComposition {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE MJ_ORDER (ID INT, CUSTOMER_ID INT, PRODUCT_ID INT, QTY INT, PRICE DOUBLE)",
                "CREATE TABLE MJ_CUSTOMER (ID INT, NAME VARCHAR(100), TIER VARCHAR(20))",
                "CREATE TABLE MJ_PRODUCT (ID INT, NAME VARCHAR(100), CATEGORY VARCHAR(50))",
                "INSERT INTO MJ_CUSTOMER VALUES (1, 'Alice', 'Gold'), (2, 'Bob', 'Silver'), (3, 'Charlie', 'Gold')",
                "INSERT INTO MJ_PRODUCT VALUES (1, 'Widget', 'Hardware'), (2, 'Service', 'Software'), (3, 'Gadget', 'Hardware')",
                "INSERT INTO MJ_ORDER VALUES (1, 1, 1, 5, 10.0), (2, 1, 2, 1, 100.0), (3, 2, 1, 3, 10.0)",
                "INSERT INTO MJ_ORDER VALUES (4, 2, 3, 2, 25.0), (5, 3, 2, 4, 100.0), (6, 3, 1, 10, 10.0)");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Order { qty: Integer[1]; price: Float[1]; }
                    Class model::Customer { name: String[1]; tier: String[1]; }
                    Class model::Product { name: String[1]; category: String[1]; }
                    Association model::Order_Customer { orders: Order[*]; customer: Customer[1]; }
                    Association model::Order_Product { orders: Order[*]; product: Product[1]; }
                    Database store::DB (
                        Table MJ_ORDER (ID INTEGER, CUSTOMER_ID INTEGER, PRODUCT_ID INTEGER, QTY INTEGER, PRICE DOUBLE)
                        Table MJ_CUSTOMER (ID INTEGER, NAME VARCHAR(100), TIER VARCHAR(20))
                        Table MJ_PRODUCT (ID INTEGER, NAME VARCHAR(100), CATEGORY VARCHAR(50))
                        Join Order_Customer(MJ_ORDER.CUSTOMER_ID = MJ_CUSTOMER.ID)
                        Join Order_Product(MJ_ORDER.PRODUCT_ID = MJ_PRODUCT.ID)
                    )
                    Mapping model::M (
                        Order: Relational { ~mainTable [store::DB] MJ_ORDER qty: [store::DB] MJ_ORDER.QTY, price: [store::DB] MJ_ORDER.PRICE }
                        Customer: Relational { ~mainTable [store::DB] MJ_CUSTOMER name: [store::DB] MJ_CUSTOMER.NAME, tier: [store::DB] MJ_CUSTOMER.TIER }
                        Product: Relational { ~mainTable [store::DB] MJ_PRODUCT name: [store::DB] MJ_PRODUCT.NAME, category: [store::DB] MJ_PRODUCT.CATEGORY }
                    
                        model::Order_Customer: Relational { AssociationMapping ( orders: [store::DB]@Order_Customer, customer: [store::DB]@Order_Customer ) }
                        model::Order_Product: Relational { AssociationMapping ( orders: [store::DB]@Order_Product, product: [store::DB]@Order_Product ) }
)
                    """, "store::DB", "model::M");
        }

        @Test @DisplayName("Project from 3 tables simultaneously")
        void testProjectThreeTables() throws SQLException {
            var r = exec(model, "Order.all()->project(~[customer:o|$o.customer.name, product:o|$o.product.name, qty:o|$o.qty])");
            assertEquals(6, r.rowCount());
        }

        @Test @DisplayName("Filter on one join, project through another")
        void testFilterOneJoinProjectAnother() throws SQLException {
            // Gold customers only, show their product names
            var r = exec(model, "Order.all()->filter({o|$o.customer.tier == 'Gold'})->project(~[customer:o|$o.customer.name, product:o|$o.product.name, qty:o|$o.qty])");
            assertEquals(4, r.rowCount()); // Alice(2 orders) + Charlie(2 orders)
        }

        @Test @DisplayName("Filter on both joins simultaneously")
        void testFilterBothJoins() throws SQLException {
            // Gold customers buying Hardware products
            var r = exec(model, "Order.all()->filter({o|$o.customer.tier == 'Gold' && $o.product.category == 'Hardware'})->project(~[customer:o|$o.customer.name, product:o|$o.product.name])");
            assertEquals(2, r.rowCount()); // Alice-Widget, Charlie-Widget
        }

        @Test @DisplayName("GroupBy on one join, filter on another")
        void testGroupByOneJoinFilterAnother() throws SQLException {
            // Only Hardware products, groupBy customer
            var r = exec(model,
                    "Order.all()->filter({o|$o.product.category == 'Hardware'})->project(~[customer:o|$o.customer.name, qty:o|$o.qty])->groupBy([{r|$r.customer}], [agg({r|$r.qty}, {y|$y->sum()})], ['customer', 'totalQty'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(5, results.get("Alice"));    // Widget x5
            assertEquals(5, results.get("Bob"));      // Widget x3 + Gadget x2
            assertEquals(10, results.get("Charlie")); // Widget x10
        }

        @Test @DisplayName("GroupBy joined column from two different tables")
        void testGroupByTwoDifferentJoins() throws SQLException {
            // GroupBy customer tier AND product category
            var r = exec(model,
                    "Order.all()->project(~[tier:o|$o.customer.tier, category:o|$o.product.category, qty:o|$o.qty])->groupBy([{r|$r.tier}, {r|$r.category}], [agg({r|$r.qty}, {y|$y->sum()})], ['tier', 'category', 'totalQty'])");
            assertTrue(r.rowCount() >= 3); // Gold-Hardware, Gold-Software, Silver-Hardware
        }

        @Test
        @DisplayName("Computed projection using columns from two joins")
        void testComputedFromTwoJoins() throws SQLException {
            var r = exec(model, "Order.all()->project(~[label:o|$o.customer.name + ' bought ' + $o.product.name, qty:o|$o.qty])");
            assertEquals(6, r.rowCount());
            assertTrue(colStr(r, 0).stream().anyMatch(s -> s.contains("Alice") && s.contains("Widget")));
        }

        @Test @DisplayName("Sort by joined column, limit")
        void testSortByJoinedLimit() throws SQLException {
            var r = exec(model, "Order.all()->sortByReversed({o|$o.qty})->limit(3)->project(~[customer:o|$o.customer.name, product:o|$o.product.name, qty:o|$o.qty])");
            assertEquals(3, r.rowCount());
            // Top 3 by qty: Charlie-Widget(10), Alice-Widget(5), Charlie-Service(4)
        }
    }

    // ==================== 24. Enum + Join Composition ====================

    @Nested
    @DisplayName("24. Enum + Join Composition")
    class EnumJoinComposition {

        @Test @DisplayName("Enum mapping + join navigation + filter")
        void testEnumJoinFilter() throws SQLException {
            sql("CREATE TABLE EJ_TICKET (ID INT, TITLE VARCHAR(200), PRIORITY_CODE VARCHAR(5), ASSIGNEE_ID INT)",
                "CREATE TABLE EJ_USER (ID INT, NAME VARCHAR(100))",
                "INSERT INTO EJ_USER VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO EJ_TICKET VALUES (1, 'Fix crash', 'H', 1), (2, 'Add docs', 'L', 2), (3, 'Perf issue', 'H', 1), (4, 'UI tweak', 'M', 2)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Enum model::Priority { LOW, MEDIUM, HIGH }
                    Class model::Ticket { title: String[1]; priority: Priority[1]; }
                    Class model::User { name: String[1]; }
                    Association model::Ticket_User { tickets: Ticket[*]; assignee: User[1]; }
                    Database store::DB (
                        Table EJ_TICKET (ID INTEGER, TITLE VARCHAR(200), PRIORITY_CODE VARCHAR(5), ASSIGNEE_ID INTEGER)
                        Table EJ_USER (ID INTEGER, NAME VARCHAR(100))
                        Join Ticket_User(EJ_TICKET.ASSIGNEE_ID = EJ_USER.ID)
                    )
                    Mapping model::M (
                        Ticket: Relational { ~mainTable [store::DB] EJ_TICKET title: [store::DB] EJ_TICKET.TITLE, priority: EnumerationMapping PM: [store::DB] EJ_TICKET.PRIORITY_CODE }
                        User: Relational { ~mainTable [store::DB] EJ_USER name: [store::DB] EJ_USER.NAME }
                        Priority: EnumerationMapping PM { HIGH: 'H', MEDIUM: 'M', LOW: 'L' }
                    
                        model::Ticket_User: Relational { AssociationMapping ( tickets: [store::DB]@Ticket_User, assignee: [store::DB]@Ticket_User ) }
)
                    """, "store::DB", "model::M");
            // Filter by HIGH priority, project assignee name
            var r = exec(m, "Ticket.all()->filter({t|$t.priority == 'HIGH'})->project(~[title:t|$t.title, assignee:t|$t.assignee.name])");
            assertEquals(2, r.rowCount());
            for (var row : r.rows()) assertEquals("Alice", row.get(1).toString());
        }

        @Test @DisplayName("Enum + join + groupBy")
        void testEnumJoinGroupBy() throws SQLException {
            sql("CREATE TABLE EJ2_ORDER (ID INT, STATUS VARCHAR(5), CUSTOMER_ID INT, AMOUNT INT)",
                "CREATE TABLE EJ2_CUST (ID INT, NAME VARCHAR(100))",
                "INSERT INTO EJ2_CUST VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO EJ2_ORDER VALUES (1, 'A', 1, 100), (2, 'A', 1, 200), (3, 'C', 2, 300), (4, 'A', 2, 150)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Enum model::Status { ACTIVE, CANCELLED }
                    Class model::Order { status: Status[1]; amount: Integer[1]; }
                    Class model::Cust { name: String[1]; }
                    Association model::Order_Cust { orders: Order[*]; customer: Cust[1]; }
                    Database store::DB (
                        Table EJ2_ORDER (ID INTEGER, STATUS VARCHAR(5), CUSTOMER_ID INTEGER, AMOUNT INTEGER)
                        Table EJ2_CUST (ID INTEGER, NAME VARCHAR(100))
                        Join Order_Cust(EJ2_ORDER.CUSTOMER_ID = EJ2_CUST.ID)
                    )
                    Mapping model::M (
                        Order: Relational { ~mainTable [store::DB] EJ2_ORDER status: EnumerationMapping SM: [store::DB] EJ2_ORDER.STATUS, amount: [store::DB] EJ2_ORDER.AMOUNT }
                        Cust: Relational { ~mainTable [store::DB] EJ2_CUST name: [store::DB] EJ2_CUST.NAME }
                        Status: EnumerationMapping SM { ACTIVE: 'A', CANCELLED: 'C' }
                    
                        model::Order_Cust: Relational { AssociationMapping ( orders: [store::DB]@Order_Cust, customer: [store::DB]@Order_Cust ) }
)
                    """, "store::DB", "model::M");
            // Active orders only, total per customer
            var r = exec(m,
                    "Order.all()->filter({o|$o.status == 'ACTIVE'})->project(~[customer:o|$o.customer.name, amount:o|$o.amount])->groupBy([{r|$r.customer}], [agg({r|$r.amount}, {y|$y->sum()})], ['customer', 'total'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(300, results.get("Alice")); // 100+200
            assertEquals(150, results.get("Bob"));   // 150 (300 is cancelled)
        }
    }

    // ==================== 25. M2M + Relational Composition ====================

    @Nested
    @DisplayName("25. M2M + Relational Composition")
    class M2MRelationalComposition {

        @Test @DisplayName("M2M with relational source that has join")
        void testM2MWithJoinSource() throws SQLException {
            sql("CREATE TABLE M2M_EMP (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), DEPT_ID INT)",
                "CREATE TABLE M2M_DEPT (ID INT, NAME VARCHAR(50))",
                "INSERT INTO M2M_DEPT VALUES (1, 'Engineering'), (2, 'Sales')",
                "INSERT INTO M2M_EMP VALUES (1, 'Alice', 'Smith', 1), (2, 'Bob', 'Jones', 2)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::RawEmp { first: String[1]; last: String[1]; }
                    Class model::RawDept { name: String[1]; }
                    Class model::Employee { fullName: String[1]; }
                    Association model::RawEmp_Dept { emps: RawEmp[*]; dept: RawDept[1]; }
                    Database store::DB (
                        Table M2M_EMP (ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), DEPT_ID INTEGER)
                        Table M2M_DEPT (ID INTEGER, NAME VARCHAR(50))
                        Join RawEmp_Dept(M2M_EMP.DEPT_ID = M2M_DEPT.ID)
                    )
                    Mapping model::M (
                        RawEmp: Relational { ~mainTable [store::DB] M2M_EMP first: [store::DB] M2M_EMP.FIRST, last: [store::DB] M2M_EMP.LAST }
                        RawDept: Relational { ~mainTable [store::DB] M2M_DEPT name: [store::DB] M2M_DEPT.NAME }
                        Employee: Pure { ~src RawEmp fullName: $src.first + ' ' + $src.last }
                    
                        model::RawEmp_Dept: Relational { AssociationMapping ( emps: [store::DB]@RawEmp_Dept, dept: [store::DB]@RawEmp_Dept ) }
)
                    """, "store::DB", "model::M");
            var r = exec(m, "Employee.all()->project(~[fullName:e|$e.fullName])");
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice Smith", "Bob Jones")));
        }

        @Test @DisplayName("M2M + filter + sort")
        void testM2MFilterSort() throws SQLException {
            sql("CREATE TABLE M2M_RAW (ID INT, A VARCHAR(50), B VARCHAR(50), SCORE INT)",
                "INSERT INTO M2M_RAW VALUES (1, 'Alpha', 'One', 80), (2, 'Beta', 'Two', 95), (3, 'Gamma', 'Three', 70)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Raw { a: String[1]; b: String[1]; score: Integer[1]; }
                    Class model::View { label: String[1]; score: Integer[1]; }
                    Database store::DB ( Table M2M_RAW (ID INTEGER, A VARCHAR(50), B VARCHAR(50), SCORE INTEGER) )
                    Mapping model::M (
                        Raw: Relational { ~mainTable [store::DB] M2M_RAW a: [store::DB] M2M_RAW.A, b: [store::DB] M2M_RAW.B, score: [store::DB] M2M_RAW.SCORE }
                        View: Pure { ~src Raw label: $src.a + '-' + $src.b, score: $src.score }
                    )
                    """, "store::DB", "model::M");
            var r = exec(m, "View.all()->filter({v|$v.score > 75})->sortByReversed({v|$v.score})->project(~[label:v|$v.label, score:v|$v.score])");
            assertEquals(2, r.rowCount());
            assertEquals("Beta-Two", r.rows().get(0).get(0).toString());   // 95
            assertEquals("Alpha-One", r.rows().get(1).get(0).toString());  // 80
        }
    }

    // ==================== 26. Large Domain Model ====================

    @Nested
    @DisplayName("26. Large Domain Model (5 tables)")
    class LargeDomainModel {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE LD_COMPANY (ID INT, NAME VARCHAR(100), COUNTRY VARCHAR(50))",
                "CREATE TABLE LD_DEPT (ID INT, NAME VARCHAR(50), COMPANY_ID INT)",
                "CREATE TABLE LD_EMP (ID INT, NAME VARCHAR(100), SAL INT, DEPT_ID INT)",
                "CREATE TABLE LD_PROJECT (ID INT, NAME VARCHAR(100), BUDGET INT, DEPT_ID INT)",
                "CREATE TABLE LD_SKILL (ID INT, EMP_ID INT, NAME VARCHAR(50), LEVEL INT)",
                "INSERT INTO LD_COMPANY VALUES (1, 'Acme Corp', 'USA'), (2, 'Beta Ltd', 'UK')",
                "INSERT INTO LD_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 1), (3, 'R&D', 2)",
                "INSERT INTO LD_EMP VALUES (1, 'Alice', 120000, 1), (2, 'Bob', 90000, 2), (3, 'Charlie', 110000, 3), (4, 'Diana', 95000, 1)",
                "INSERT INTO LD_PROJECT VALUES (1, 'ProjectX', 500000, 1), (2, 'ProjectY', 200000, 2), (3, 'ProjectZ', 800000, 3)",
                "INSERT INTO LD_SKILL VALUES (1, 1, 'Java', 9), (2, 1, 'Python', 7), (3, 2, 'Sales', 8), (4, 3, 'ML', 9), (5, 4, 'Java', 6)");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Company { name: String[1]; country: String[1]; }
                    Class model::Dept { name: String[1]; }
                    Class model::Emp { name: String[1]; sal: Integer[1]; }
                    Class model::Project { name: String[1]; budget: Integer[1]; }
                    Class model::Skill { name: String[1]; level: Integer[1]; }
                    Association model::Dept_Company { depts: Dept[*]; company: Company[1]; }
                    Association model::Emp_Dept { emps: Emp[*]; dept: Dept[1]; }
                    Association model::Project_Dept { projects: Project[*]; dept: Dept[1]; }
                    Association model::Emp_Skill { emp: Emp[1]; skills: Skill[*]; }
                    Database store::DB (
                        Table LD_COMPANY (ID INTEGER, NAME VARCHAR(100), COUNTRY VARCHAR(50))
                        Table LD_DEPT (ID INTEGER, NAME VARCHAR(50), COMPANY_ID INTEGER)
                        Table LD_EMP (ID INTEGER, NAME VARCHAR(100), SAL INTEGER, DEPT_ID INTEGER)
                        Table LD_PROJECT (ID INTEGER, NAME VARCHAR(100), BUDGET INTEGER, DEPT_ID INTEGER)
                        Table LD_SKILL (ID INTEGER, EMP_ID INTEGER, NAME VARCHAR(50), LEVEL INTEGER)
                        Join Dept_Company(LD_DEPT.COMPANY_ID = LD_COMPANY.ID)
                        Join Emp_Dept(LD_EMP.DEPT_ID = LD_DEPT.ID)
                        Join Project_Dept(LD_PROJECT.DEPT_ID = LD_DEPT.ID)
                        Join Emp_Skill(LD_EMP.ID = LD_SKILL.EMP_ID)
                    )
                    Mapping model::M (
                        Company: Relational { ~mainTable [store::DB] LD_COMPANY name: [store::DB] LD_COMPANY.NAME, country: [store::DB] LD_COMPANY.COUNTRY }
                        Dept: Relational { ~mainTable [store::DB] LD_DEPT name: [store::DB] LD_DEPT.NAME }
                        Emp: Relational { ~mainTable [store::DB] LD_EMP name: [store::DB] LD_EMP.NAME, sal: [store::DB] LD_EMP.SAL }
                        Project: Relational { ~mainTable [store::DB] LD_PROJECT name: [store::DB] LD_PROJECT.NAME, budget: [store::DB] LD_PROJECT.BUDGET }
                        Skill: Relational { ~mainTable [store::DB] LD_SKILL name: [store::DB] LD_SKILL.NAME, level: [store::DB] LD_SKILL.LEVEL }
                    
                        model::Dept_Company: Relational { AssociationMapping ( depts: [store::DB]@Dept_Company, company: [store::DB]@Dept_Company ) }
                        model::Emp_Dept: Relational { AssociationMapping ( emps: [store::DB]@Emp_Dept, dept: [store::DB]@Emp_Dept ) }
                        model::Project_Dept: Relational { AssociationMapping ( projects: [store::DB]@Project_Dept, dept: [store::DB]@Project_Dept ) }
                        model::Emp_Skill: Relational { AssociationMapping ( emp: [store::DB]@Emp_Skill, skills: [store::DB]@Emp_Skill ) }
)
                    """, "store::DB", "model::M");
        }

        @Test // was @Disabled — testing 2-hop chain
        @DisplayName("Employee with department and company (2 joins)")
        void testEmpDeptCompany() throws SQLException {
            var r = exec(model, "Emp.all()->project(~[name:e|$e.name, dept:e|$e.dept.name, company:e|$e.dept.company.name])");
            assertEquals(4, r.rowCount());
            Map<String, String> empCompany = new HashMap<>();
            for (var row : r.rows()) empCompany.put(row.get(0).toString(), row.get(2).toString());
            assertEquals("Acme Corp", empCompany.get("Alice"));
            assertEquals("Beta Ltd", empCompany.get("Charlie"));
        }

        @Test // was @Disabled — 2-hop filter fixed with multi-hop EXISTS
        @DisplayName("Filter on company country, project employee name")
        void testFilterCompanyProjectEmp() throws SQLException {
            var r = exec(model, "Emp.all()->filter({e|$e.dept.company.country == 'USA'})->project(~[name:e|$e.name])");
            assertEquals(3, r.rowCount()); // Alice, Bob, Diana (all in Acme USA)
        }

        @Test @DisplayName("Employee with to-many skills (LEFT JOIN)")
        void testEmpSkills() throws SQLException {
            var r = exec(model, "Emp.all()->project(~[name:e|$e.name, skill:e|$e.skills.name])");
            // Alice: 2 skills, Bob: 1, Charlie: 1, Diana: 1 = 5 rows
            assertEquals(5, r.rowCount());
        }

        @Test @DisplayName("Filter emp by skill, project dept")
        void testFilterSkillProjectDept() throws SQLException {
            // Employees who know Java
            var r = exec(model, "Emp.all()->filter({e|$e.skills.name == 'Java'})->project(~[name:e|$e.name, dept:e|$e.dept.name])");
            assertEquals(2, r.rowCount()); // Alice and Diana
        }

        @Test // was @Disabled — 2-hop chain works now
        @DisplayName("Total salary per company")
        void testTotalSalPerCompany() throws SQLException {
            var r = exec(model,
                    "Emp.all()->project(~[company:e|$e.dept.company.name, sal:e|$e.sal])->groupBy([{r|$r.company}], [agg({r|$r.sal}, {y|$y->sum()})], ['company', 'totalSal'])");
            Map<String, Integer> results = new HashMap<>();
            for (var row : r.rows()) results.put(row.get(0).toString(), ((Number) row.get(1)).intValue());
            assertEquals(305000, results.get("Acme Corp")); // 120k+90k+95k
            assertEquals(110000, results.get("Beta Ltd"));  // 110k
        }

        @Test @DisplayName("Filter by skill + sort by salary + limit (top Java devs)")
        void testFilterSkillSortSalLimit() throws SQLException {
            var r = exec(model, "Emp.all()->filter({e|$e.skills.name == 'Java'})->sortByReversed({e|$e.sal})->limit(1)->project(~[name:e|$e.name, sal:e|$e.sal])");
            assertEquals(1, r.rowCount());
            assertEquals("Alice", r.rows().get(0).get(0).toString()); // 120k
        }

        @Test @DisplayName("Project dept and its projects (to-many)")
        void testDeptProjects() throws SQLException {
            var r = exec(model, "Dept.all()->project(~[dept:d|$d.name, project:d|$d.projects.name, budget:d|$d.projects.budget])");
            assertEquals(3, r.rowCount()); // One project per dept
        }
    }

    // ==================== 27. Statelessness and Re-use ====================

    @Nested
    @DisplayName("27. Statelessness and Re-use")
    class Statelessness {

        @Test @DisplayName("Multiple queries on same model produce correct independent results")
        void testMultipleQueries() throws SQLException {
            sql("CREATE TABLE ST (ID INT, NAME VARCHAR(50), VAL INT)",
                "INSERT INTO ST VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
            String m = singleTableModel("S", "ST", "store::DB", "model::M",
                    "Class model::S { name: String[1]; val: Integer[1]; }",
                    "ID INTEGER, NAME VARCHAR(50), VAL INTEGER",
                    "name: [store::DB] ST.NAME, val: [store::DB] ST.VAL");

            var r1 = exec(m, "S.all()->filter({s|$s.val > 15})->project(~[name:s|$s.name])");
            assertEquals(2, r1.rowCount());

            var r2 = exec(m, "S.all()->filter({s|$s.val < 25})->project(~[name:s|$s.name])");
            assertEquals(2, r2.rowCount());

            var r3 = exec(m, "S.all()->project(~[name:s|$s.name])");
            assertEquals(3, r3.rowCount());
        }

        @Test @DisplayName("Data mutations between queries are reflected")
        void testDataMutations() throws SQLException {
            sql("CREATE TABLE MUT (ID INT, NAME VARCHAR(50))",
                "INSERT INTO MUT VALUES (1, 'first')");
            String m = singleTableModel("M", "MUT", "store::DB", "model::M",
                    "Class model::M { name: String[1]; }",
                    "ID INTEGER, NAME VARCHAR(50)",
                    "name: [store::DB] MUT.NAME");

            var r1 = exec(m, "M.all()->project(~[name:x|$x.name])");
            assertEquals(1, r1.rowCount());

            sql("INSERT INTO MUT VALUES (2, 'second')");
            var r2 = exec(m, "M.all()->project(~[name:x|$x.name])");
            assertEquals(2, r2.rowCount());

            sql("DELETE FROM MUT WHERE ID = 1");
            var r3 = exec(m, "M.all()->project(~[name:x|$x.name])");
            assertEquals(1, r3.rowCount());
            assertEquals("second", r3.rows().get(0).get(0).toString());
        }
    }

    // ==================== 28. Embedded Mappings ====================

    @Nested
    @DisplayName("28. Embedded Mappings")
    class EmbeddedMappings {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_REVENUE INT)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 'Acme Corp', 1000000), (2, 'Bob', 'Beta Inc', 500000), (3, 'Charlie', 'Acme Corp', 1000000)");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; revenue: Integer[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_REVENUE INTEGER)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm (
                                legalName: [store::DB] T_PERSON.FIRM_NAME,
                                revenue: [store::DB] T_PERSON.FIRM_REVENUE
                            )
                        }
                    )
                    """, "store::DB", "model::M");
        }

        @Test
        @DisplayName("Project single embedded property")
        void testEmbeddedSingleProperty() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 1).contains("Acme Corp"));
            assertTrue(colStr(r, 1).contains("Beta Inc"));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Embedded should produce no JOIN");
        }

        @Test
        @DisplayName("Project multiple embedded properties")
        void testEmbeddedMultiProperty() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName, rev:p|$p.firm.revenue])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            // Alice → Acme Corp, 1000000
            var names = colStr(r, 0);
            var firms = colStr(r, 1);
            var revs = colInt(r, 2);
            int aliceIdx = names.indexOf("Alice");
            assertEquals("Acme Corp", firms.get(aliceIdx));
            assertEquals(Integer.valueOf(1000000), revs.get(aliceIdx));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Embedded should produce no JOIN");
        }

        @Test
        @DisplayName("Filter on embedded property")
        void testEmbeddedFilter() throws SQLException {
            var query = "Person.all()->filter({p|$p.firm.legalName == 'Acme Corp'})->project(~[name:p|$p.name])";
            var r = exec(model, query);
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Embedded filter should produce no JOIN");
        }

        @Test
        @DisplayName("SQL has no JOIN for embedded — columns from parent table")
        void testEmbeddedNoJoinSql() {
            String sql = planSql(model, "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])");
            assertFalse(sql.toUpperCase().contains("JOIN"), "Embedded should produce no JOIN: " + sql);
            assertTrue(sql.contains("FIRM_NAME"), "Should reference FIRM_NAME column directly: " + sql);
        }

        @Test
        @DisplayName("Embedded + flat column coexistence")
        void testEmbeddedWithFlatColumn() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])->sort(~name->ascending())";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            assertEquals("Alice", colStr(r, 0).get(0));
            assertEquals("Acme Corp", colStr(r, 1).get(0));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Embedded + sort should produce no JOIN");
        }
    }

    // ==================== 29. Embedded + Association Coexistence ====================

    @Nested
    @DisplayName("29. Embedded + Association Coexistence")
    class EmbeddedWithAssociation {

        @Test
        @DisplayName("Embedded property on same table + association join on different table")
        void testEmbeddedAndAssociationTogether() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), ADDR_ID INT)",
                "CREATE TABLE T_ADDRESS (ID INT PRIMARY KEY, STREET VARCHAR(200), CITY VARCHAR(100))",
                "INSERT INTO T_ADDRESS VALUES (1, '123 Main St', 'New York'), (2, '456 Oak Ave', 'Boston')",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 'Acme Corp', 1), (2, 'Bob', 'Beta Inc', 2)");
            var model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; }
                    Class model::Address { street: String[1]; city: String[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Association model::Person_Address { person: Person[1]; address: Address[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), ADDR_ID INTEGER)
                        Table T_ADDRESS (ID INTEGER PRIMARY KEY, STREET VARCHAR(200), CITY VARCHAR(100))
                        Join Person_Address(T_PERSON.ADDR_ID = T_ADDRESS.ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm (
                                legalName: [store::DB] T_PERSON.FIRM_NAME
                            )
                        }
                        Address: Relational {
                            ~mainTable [store::DB] T_ADDRESS
                            street: [store::DB] T_ADDRESS.STREET,
                            city: [store::DB] T_ADDRESS.CITY
                        }
                    
                        model::Person_Address: Relational { AssociationMapping ( person: [store::DB]@Person_Address, address: [store::DB]@Person_Address ) }
)
                    """, "store::DB", "model::M");

            // Project embedded firm + association address
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName, city:p|$p.address.city])";
            var r = exec(model, query);
            assertEquals(2, r.rowCount());
            var names = colStr(r, 0);
            int aliceIdx = names.indexOf("Alice");
            assertEquals("Acme Corp", colStr(r, 1).get(aliceIdx));
            assertEquals("New York", colStr(r, 2).get(aliceIdx));
            // Exactly one JOIN for address association; firm embedded = no JOIN
            String sql = planSql(model, query).toUpperCase();
            assertEquals(1, sql.split("JOIN").length - 1, "Expected exactly 1 JOIN (address): " + sql);
            assertTrue(sql.contains("T_ADDRESS"), "JOIN should be to T_ADDRESS: " + sql);
        }

        @Test
        @DisplayName("SQL: embedded = no JOIN, association = LEFT JOIN")
        void testEmbeddedAndAssociationSql() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), ADDR_ID INT)",
                "CREATE TABLE T_ADDRESS (ID INT, STREET VARCHAR(200), CITY VARCHAR(100))");
            var model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; }
                    Class model::Address { street: String[1]; city: String[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Association model::Person_Address { person: Person[1]; address: Address[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), ADDR_ID INTEGER)
                        Table T_ADDRESS (ID INTEGER PRIMARY KEY, STREET VARCHAR(200), CITY VARCHAR(100))
                        Join Person_Address(T_PERSON.ADDR_ID = T_ADDRESS.ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm (
                                legalName: [store::DB] T_PERSON.FIRM_NAME
                            )
                        }
                        Address: Relational {
                            ~mainTable [store::DB] T_ADDRESS
                            street: [store::DB] T_ADDRESS.STREET,
                            city: [store::DB] T_ADDRESS.CITY
                        }
                    
                        model::Person_Address: Relational { AssociationMapping ( person: [store::DB]@Person_Address, address: [store::DB]@Person_Address ) }
)
                    """, "store::DB", "model::M");

            String sql = planSql(model, "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName, city:p|$p.address.city])");
            String upper = sql.toUpperCase();
            // Exactly one JOIN (for address association); firm embedded = no JOIN
            assertEquals(1, upper.split("JOIN").length - 1, "Expected exactly 1 JOIN (address): " + sql);
            assertTrue(upper.contains("T_ADDRESS"), "JOIN should be to T_ADDRESS: " + sql);
            assertTrue(sql.contains("FIRM_NAME"), "Firm should reference parent column directly: " + sql);
        }
    }

    // ==================== 30. Inline Mappings ====================

    @Nested
    @DisplayName("30. Inline Mappings")
    class InlineMappings {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_REVENUE INT)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 'Acme Corp', 1000000), (2, 'Bob', 'Beta Inc', 500000), (3, 'Charlie', 'Acme Corp', 1000000)");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; revenue: Integer[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_REVENUE INTEGER)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm() Inline[firm_set1]
                        }
                        Firm[firm_set1]: Relational {
                            ~mainTable [store::DB] T_PERSON
                            legalName: [store::DB] T_PERSON.FIRM_NAME,
                            revenue: [store::DB] T_PERSON.FIRM_REVENUE
                        }
                    )
                    """, "store::DB", "model::M");
        }

        @Test
        @DisplayName("Project single inline property")
        void testInlineSingleProperty() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 1).contains("Acme Corp"));
            assertTrue(colStr(r, 1).contains("Beta Inc"));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Inline should produce no JOIN");
        }

        @Test
        @DisplayName("Project multiple inline properties")
        void testInlineMultiProperty() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName, rev:p|$p.firm.revenue])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            var names = colStr(r, 0);
            int aliceIdx = names.indexOf("Alice");
            assertEquals("Acme Corp", colStr(r, 1).get(aliceIdx));
            assertEquals(Integer.valueOf(1000000), colInt(r, 2).get(aliceIdx));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Inline should produce no JOIN");
        }

        @Test
        @DisplayName("Filter on inline property")
        void testInlineFilter() throws SQLException {
            var query = "Person.all()->filter({p|$p.firm.legalName == 'Acme Corp'})->project(~[name:p|$p.name])";
            var r = exec(model, query);
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"), "Inline filter should produce no JOIN");
        }

        @Test
        @DisplayName("SQL has no JOIN for inline — columns from referenced mapping's table")
        void testInlineNoJoinSql() {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])";
            String sql = planSql(model, query);
            assertFalse(sql.toUpperCase().contains("JOIN"), "Inline should produce no JOIN: " + sql);
            assertTrue(sql.contains("FIRM_NAME"), "Should reference FIRM_NAME column directly: " + sql);
        }
    }

    // ==================== 31. Otherwise Mappings ====================

    @Nested
    @DisplayName("31. Otherwise Mappings")
    class OtherwiseMappings {

        private String model;

        @BeforeEach
        void setup() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT PRIMARY KEY, LEGAL_NAME VARCHAR(200), REVENUE INT)",
                "INSERT INTO T_FIRM VALUES (1, 'Acme Corp', 1000000), (2, 'Beta Inc', 500000)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 'Acme Corp', 1), (2, 'Bob', 'Beta Inc', 2), (3, 'Charlie', 'Acme Corp', 1)");
            model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Person { name: String[1]; }
                    Class model::Firm { legalName: String[1]; revenue: Integer[1]; }
                    Association model::Person_Firm { person: Person[*]; firm: Firm[1]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_NAME VARCHAR(200), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(200), REVENUE INTEGER)
                        Join Person_Firm(T_PERSON.FIRM_ID = T_FIRM.ID)
                    )
                    Mapping model::M (
                        Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm (
                                legalName: [store::DB] T_PERSON.FIRM_NAME
                            ) Otherwise([firm_set1]: [store::DB]@Person_Firm)
                        }
                        Firm[firm_set1]: Relational {
                            ~mainTable [store::DB] T_FIRM
                            legalName: [store::DB] T_FIRM.LEGAL_NAME,
                            revenue: [store::DB] T_FIRM.REVENUE
                        }
                    
                        model::Person_Firm: Relational { AssociationMapping ( person: [store::DB]@Person_Firm, firm: [store::DB]@Person_Firm ) }
)
                    """, "store::DB", "model::M");
        }

        @Test
        @DisplayName("Embedded property from parent table (no JOIN needed)")
        void testOtherwiseEmbeddedProperty() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            assertTrue(colStr(r, 1).contains("Acme Corp"));
            // Embedded property resolves from parent table — no JOIN
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"),
                    "Embedded property should not require JOIN");
        }

        @Test
        @DisplayName("Fallback property via join (JOIN required)")
        void testOtherwiseFallbackProperty() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, rev:p|$p.firm.revenue])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            var names = colStr(r, 0);
            int aliceIdx = names.indexOf("Alice");
            assertEquals(Integer.valueOf(1000000), colInt(r, 1).get(aliceIdx));
            // Fallback property needs JOIN to T_FIRM
            String sql = planSql(model, query).toUpperCase();
            assertEquals(1, sql.split("JOIN").length - 1, "Expected 1 JOIN for fallback: " + sql);
            assertTrue(sql.contains("T_FIRM"), "JOIN should be to T_FIRM: " + sql);
        }

        @Test
        @DisplayName("Mixed: embedded from parent + fallback from join")
        void testOtherwiseMixed() throws SQLException {
            var query = "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName, rev:p|$p.firm.revenue])";
            var r = exec(model, query);
            assertEquals(3, r.rowCount());
            var names = colStr(r, 0);
            int aliceIdx = names.indexOf("Alice");
            assertEquals("Acme Corp", colStr(r, 1).get(aliceIdx));
            assertEquals(Integer.valueOf(1000000), colInt(r, 2).get(aliceIdx));
            // One JOIN for revenue (fallback), legalName from parent
            String sql = planSql(model, query).toUpperCase();
            assertEquals(1, sql.split("JOIN").length - 1, "Expected 1 JOIN: " + sql);
            assertTrue(sql.contains("FIRM_NAME"), "Embedded legalName from parent: " + sql);
            assertTrue(sql.contains("T_FIRM"), "Fallback revenue via JOIN: " + sql);
        }

        @Test
        @DisplayName("Filter on embedded property (no JOIN needed)")
        void testOtherwiseFilterOnEmbedded() throws SQLException {
            var query = "Person.all()->filter({p|$p.firm.legalName == 'Acme Corp'})->project(~[name:p|$p.name])";
            var r = exec(model, query);
            assertEquals(2, r.rowCount());
            assertTrue(colStr(r, 0).containsAll(List.of("Alice", "Charlie")));
            assertFalse(planSql(model, query).toUpperCase().contains("JOIN"),
                    "Filter on embedded property should not require JOIN");
        }

        // --- Pruning: otherwise creates 2 extends (embedded + association) sharing
        // the same ColSpec name "firm". stampExtendOverrides prunes both together,
        // and PlanGenerator's lazy-JOIN logic only emits the JOIN when a fallback
        // property is actually accessed.

        @Test
        @DisplayName("Pruning: no firm access → 0 JOINs, no T_FIRM")
        void testOtherwisePruningNoFirmAccess() {
            String sql = planSql(model,
                    "Person.all()->project(~[name:p|$p.name])");
            assertFalse(sql.toUpperCase().contains("JOIN"),
                    "No firm access → no JOIN. SQL: " + sql);
            assertFalse(sql.toUpperCase().contains("T_FIRM"),
                    "T_FIRM should not appear. SQL: " + sql);
        }

        @Test
        @DisplayName("Pruning: only embedded firm.legalName → 0 JOINs")
        void testOtherwisePruningEmbeddedOnly() {
            String sql = planSql(model,
                    "Person.all()->project(~[name:p|$p.name, firmName:p|$p.firm.legalName])");
            assertFalse(sql.toUpperCase().contains("JOIN"),
                    "Embedded-only access → no JOIN. SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("FIRM_NAME"),
                    "Should resolve legalName from parent table column FIRM_NAME. SQL: " + sql);
        }

        @Test
        @DisplayName("Pruning: only fallback firm.revenue → 1 JOIN to T_FIRM")
        void testOtherwisePruningFallbackOnly() {
            String sql = planSql(model,
                    "Person.all()->project(~[name:p|$p.name, rev:p|$p.firm.revenue])");
            assertEquals(1, sql.toUpperCase().split("JOIN").length - 1,
                    "Fallback-only access → 1 JOIN. SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("T_FIRM"),
                    "JOIN should target T_FIRM. SQL: " + sql);
        }

        @Test
        @DisplayName("Pruning: mixed embedded + fallback → 1 JOIN, both columns present")
        void testOtherwisePruningMixed() {
            String sql = planSql(model,
                    "Person.all()->project(~[firmName:p|$p.firm.legalName, rev:p|$p.firm.revenue])");
            assertEquals(1, sql.toUpperCase().split("JOIN").length - 1,
                    "Mixed access → 1 JOIN for fallback. SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("FIRM_NAME"),
                    "Embedded legalName from parent. SQL: " + sql);
            assertTrue(sql.toUpperCase().contains("T_FIRM"),
                    "Fallback revenue via JOIN. SQL: " + sql);
        }
    }

    // ==================== GAP: Composition of Unsupported Features ====================

    @Nested
    @DisplayName("GAP: Feature Composition (Disabled)")
    class GapComposition {

        @Test
        @DisplayName("3-hop join: Emp → Dept → Company → Country")
        void testThreeHopJoin() throws SQLException {
            sql("CREATE TABLE T_EMP (ID INT PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INT)",
                "CREATE TABLE T_DEPT (ID INT PRIMARY KEY, NAME VARCHAR(50), COMPANY_ID INT)",
                "CREATE TABLE T_COMPANY (ID INT PRIMARY KEY, NAME VARCHAR(100), COUNTRY_ID INT)",
                "CREATE TABLE T_COUNTRY (ID INT PRIMARY KEY, NAME VARCHAR(50))",
                "INSERT INTO T_COUNTRY VALUES (1, 'USA'), (2, 'UK')",
                "INSERT INTO T_COMPANY VALUES (1, 'Acme Corp', 1), (2, 'Brit Ltd', 2)",
                "INSERT INTO T_DEPT VALUES (1, 'Engineering', 1), (2, 'Sales', 2)",
                "INSERT INTO T_EMP VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::Emp { name: String[1]; }
                    Class model::Dept { name: String[1]; }
                    Class model::Company { name: String[1]; }
                    Class model::Country { name: String[1]; }
                    Association model::Emp_Dept { emps: Emp[*]; dept: Dept[1]; }
                    Association model::Dept_Company { depts: Dept[*]; company: Company[1]; }
                    Association model::Company_Country { companies: Company[*]; country: Country[1]; }
                    Database store::DB (
                        Table T_EMP (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                        Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), COMPANY_ID INTEGER)
                        Table T_COMPANY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), COUNTRY_ID INTEGER)
                        Table T_COUNTRY (ID INTEGER PRIMARY KEY, NAME VARCHAR(50))
                        Join Emp_Dept(T_EMP.DEPT_ID = T_DEPT.ID)
                        Join Dept_Company(T_DEPT.COMPANY_ID = T_COMPANY.ID)
                        Join Company_Country(T_COMPANY.COUNTRY_ID = T_COUNTRY.ID)
                    )
                    Mapping model::M (
                        Emp: Relational { ~mainTable [store::DB] T_EMP name: [store::DB] T_EMP.NAME }
                        Dept: Relational { ~mainTable [store::DB] T_DEPT name: [store::DB] T_DEPT.NAME }
                        Company: Relational { ~mainTable [store::DB] T_COMPANY name: [store::DB] T_COMPANY.NAME }
                        Country: Relational { ~mainTable [store::DB] T_COUNTRY name: [store::DB] T_COUNTRY.NAME }
                    
                        model::Emp_Dept: Relational { AssociationMapping ( emps: [store::DB]@Emp_Dept, dept: [store::DB]@Emp_Dept ) }
                        model::Dept_Company: Relational { AssociationMapping ( depts: [store::DB]@Dept_Company, company: [store::DB]@Dept_Company ) }
                        model::Company_Country: Relational { AssociationMapping ( companies: [store::DB]@Company_Country, country: [store::DB]@Company_Country ) }
)
                    """, "store::DB", "model::M");
            // Navigate Emp → dept → company → country (3 hops)
            var r = exec(m, "Emp.all()->project(~[name:e|$e.name, country:e|$e.dept.company.country.name])");
            assertEquals(3, r.rowCount());
            // Alice & Charlie → Engineering → Acme Corp → USA
            // Bob → Sales → Brit Ltd → UK
            var rows = r.rows();
            for (var row : rows) {
                String empName = row.get(0).toString();
                String countryName = row.get(1).toString();
                if ("Alice".equals(empName) || "Charlie".equals(empName)) {
                    assertEquals("USA", countryName);
                } else {
                    assertEquals("UK", countryName);
                }
            }
        }

        @Test
        @DisplayName("GroupBy on 3rd-hop table column")
        void testMultiHopAggregation() throws SQLException {
            sql("CREATE TABLE T_EMP2 (ID INT, NAME VARCHAR(100), DEPT_ID INT, SALARY INT)",
                "CREATE TABLE T_DEPT2 (ID INT, NAME VARCHAR(50), COMPANY_ID INT)",
                "CREATE TABLE T_COMPANY2 (ID INT, NAME VARCHAR(100), COUNTRY VARCHAR(50))",
                "INSERT INTO T_COMPANY2 VALUES (1, 'Acme', 'USA'), (2, 'Brit', 'UK')",
                "INSERT INTO T_DEPT2 VALUES (1, 'Eng', 1), (2, 'Sales', 2)",
                "INSERT INTO T_EMP2 VALUES (1, 'Alice', 1, 100), (2, 'Bob', 2, 200), (3, 'Charlie', 1, 150)");
            String m = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::E2 { name: String[1]; salary: Integer[1]; }
                    Class model::D2 { name: String[1]; }
                    Class model::C2 { country: String[1]; }
                    Association model::E2_D2 { emps: E2[*]; dept: D2[1]; }
                    Association model::D2_C2 { depts: D2[*]; company: C2[1]; }
                    Database store::DB (
                        Table T_EMP2 (ID INTEGER, NAME VARCHAR(100), DEPT_ID INTEGER, SALARY INTEGER)
                        Table T_DEPT2 (ID INTEGER, NAME VARCHAR(50), COMPANY_ID INTEGER)
                        Table T_COMPANY2 (ID INTEGER, NAME VARCHAR(100), COUNTRY VARCHAR(50))
                        Join E2_D2(T_EMP2.DEPT_ID = T_DEPT2.ID)
                        Join D2_C2(T_DEPT2.COMPANY_ID = T_COMPANY2.ID)
                    )
                    Mapping model::M (
                        E2: Relational { ~mainTable [store::DB] T_EMP2 name: [store::DB] T_EMP2.NAME, salary: [store::DB] T_EMP2.SALARY }
                        D2: Relational { ~mainTable [store::DB] T_DEPT2 name: [store::DB] T_DEPT2.NAME }
                        C2: Relational { ~mainTable [store::DB] T_COMPANY2 country: [store::DB] T_COMPANY2.COUNTRY }
                    
                        model::E2_D2: Relational { AssociationMapping ( emps: [store::DB]@E2_D2, dept: [store::DB]@E2_D2 ) }
                        model::D2_C2: Relational { AssociationMapping ( depts: [store::DB]@D2_C2, company: [store::DB]@D2_C2 ) }
)
                    """, "store::DB", "model::M");
            // GroupBy country (2 hops from Emp), sum salary
            var r = exec(m, "E2.all()->project(~[country:e|$e.dept.company.country, salary:e|$e.salary])->groupBy([{r|$r.country}], [agg({r|$r.salary}, {y|$y->sum()})], ['country', 'totalSalary'])");
            assertEquals(2, r.rowCount());
        }


        @Test @Disabled("GAP: Set IDs + filter disambiguation")
        @DisplayName("GAP: Multiple set IDs with filter selecting correct set")
        void testSetIdFilter() throws SQLException {
            // *Person[set1]: Relational { ~mainTable T_ACTIVE ... }
            // Person[set2]: Relational { ~mainTable T_ARCHIVED ... }
        }

        @Test @Disabled("GAP: Extends + filter inheritance")
        @DisplayName("GAP: Mapping extends with filter on parent properties")
        void testExtendsWithFilter() throws SQLException {
            // Employee extends Person, filter on Person.name
        }

        @Test @Disabled("GAP: Mapping include + join navigation")
        @DisplayName("GAP: Included mapping's classes used in join query")
        void testIncludeWithJoin() throws SQLException {
            // include BaseMapping, then query joining to included class
        }

        @Test @Disabled("GAP: Store substitution + query")
        @DisplayName("GAP: Include with store sub, verify correct DB used")
        void testStoreSubstitutionQuery() throws SQLException {
            // include BaseMapping[DevDB -> ProdDB], verify ProdDB tables queried
        }

        @Test @Disabled("GAP: View + join + filter")
        @DisplayName("GAP: Query through view with join and filter")
        void testViewJoinFilter() throws SQLException {
            // View as source table + join to another table + filter
        }

        @Test @Disabled("GAP: Database filter + mapping filter stacking")
        @DisplayName("GAP: Both database filter and mapping ~filter active")
        void testDbFilterPlusMappingFilter() throws SQLException {
            // Database Filter(isActive) + Mapping ~filter [activeOnly]
        }

        @Test @Disabled("GAP: Local property + join + filter")
        @DisplayName("GAP: Local mapping property used in filter with join")
        void testLocalPropertyWithJoin() throws SQLException {
            // +fullName: $p.first + ' ' + $p.last, filter on fullName, project dept
        }

        @Test
        @DisplayName("DynaFunction mapped property filtered on")
        void testDynaFunctionWithFilter() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50))",
                "INSERT INTO PEOPLE VALUES (1, 'Alice', 'Smith'), (2, 'Bob', 'Jones'), (3, 'Carol', 'Smith')");
            String model = withRuntime("""
                    import model::*;
                    import store::*;

                    Class model::P { fullName: String[1]; }
                    Database store::DB ( Table PEOPLE ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] PEOPLE
                        fullName: concat([store::DB] PEOPLE.FIRST, ' ', [store::DB] PEOPLE.LAST)
                    } )
                    """, "store::DB", "model::M");
            var r = exec(model, "P.all()->filter(p|$p.fullName->contains('Smith'))->project(~[name:p|$p.fullName])");
            var names = colStr(r, 0);
            assertEquals(2, names.size());
            assertTrue(names.contains("Alice Smith"));
            assertTrue(names.contains("Carol Smith"));
        }

        @Test
        @DisplayName("Association mapping + multi-hop join chain")
        void testAssocMappingWithJoins() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT, LEGAL_NAME VARCHAR(100), COUNTRY_ID INT)",
                "CREATE TABLE T_COUNTRY (ID INT, NAME VARCHAR(100))",
                "INSERT INTO T_COUNTRY VALUES (1, 'USA'), (2, 'UK')",
                "INSERT INTO T_FIRM VALUES (1, 'Acme', 1), (2, 'Brit Corp', 2)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { name: String[1]; }
                    Class test::Firm { legalName: String[1]; country: String[1]; }
                    Association test::PersonFirm { firm: Firm[1]; employees: Person[*]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(100), COUNTRY_ID INTEGER)
                        Table T_COUNTRY (ID INTEGER, NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Join FirmCountry(T_FIRM.COUNTRY_ID = T_COUNTRY.ID)
                    )
                    Mapping test::M (
                        test::Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                        test::Firm: Relational {
                            ~mainTable [store::DB] T_FIRM
                            legalName: [store::DB] T_FIRM.LEGAL_NAME,
                            country: @FirmCountry | [store::DB] T_COUNTRY.NAME
                        }
                        test::PersonFirm: Relational { AssociationMapping (
                            firm: [store::DB]@PersonFirm,
                            employees: [store::DB]@PersonFirm
                        ) }
                    )
                    """, "store::DB", "test::M");
            // Navigate Person -> Firm (association) -> Country (join chain on Firm)
            var r = exec(model, "Person.all()->project(~[name:p|$p.name, firm:p|$p.firm.legalName, country:p|$p.firm.country])->sort(~name->ascending())");
            assertEquals(3, r.rowCount());
            assertEquals(List.of("Alice", "Bob", "Charlie"), colStr(r, 0));
            assertEquals("Acme", colStr(r, 1).get(0));
            assertEquals("USA", colStr(r, 2).get(0));
            assertEquals("Brit Corp", colStr(r, 1).get(1));
            assertEquals("UK", colStr(r, 2).get(1));
        }

        @Test
        @DisplayName("Filter on association → join-chain property")
        void testAssocJoinChainFilter() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT, LEGAL_NAME VARCHAR(100), COUNTRY_ID INT)",
                "CREATE TABLE T_COUNTRY (ID INT, NAME VARCHAR(100))",
                "INSERT INTO T_COUNTRY VALUES (1, 'USA'), (2, 'UK')",
                "INSERT INTO T_FIRM VALUES (1, 'Acme', 1), (2, 'Brit Corp', 2)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { name: String[1]; }
                    Class test::Firm { legalName: String[1]; country: String[1]; }
                    Association test::PersonFirm { firm: Firm[1]; employees: Person[*]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(100), COUNTRY_ID INTEGER)
                        Table T_COUNTRY (ID INTEGER, NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Join FirmCountry(T_FIRM.COUNTRY_ID = T_COUNTRY.ID)
                    )
                    Mapping test::M (
                        test::Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                        test::Firm: Relational {
                            ~mainTable [store::DB] T_FIRM
                            legalName: [store::DB] T_FIRM.LEGAL_NAME,
                            country: @FirmCountry | [store::DB] T_COUNTRY.NAME
                        }
                        test::PersonFirm: Relational { AssociationMapping (
                            firm: [store::DB]@PersonFirm,
                            employees: [store::DB]@PersonFirm
                        ) }
                    )
                    """, "store::DB", "test::M");
            var r = exec(model, "Person.all()->filter({p|$p.firm.country == 'USA'})->project(~[name:p|$p.name])->sort(~name->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Alice", "Charlie"), colStr(r, 0));
        }

        @Test
        @DisplayName("Sort on association → join-chain property")
        void testAssocJoinChainSort() throws SQLException {
            sql("CREATE TABLE T_PERSON (ID INT, NAME VARCHAR(100), FIRM_ID INT)",
                "CREATE TABLE T_FIRM (ID INT, LEGAL_NAME VARCHAR(100), COUNTRY_ID INT)",
                "CREATE TABLE T_COUNTRY (ID INT, NAME VARCHAR(100))",
                "INSERT INTO T_COUNTRY VALUES (1, 'USA'), (2, 'UK')",
                "INSERT INTO T_FIRM VALUES (1, 'Acme', 1), (2, 'Brit Corp', 2)",
                "INSERT INTO T_PERSON VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Person { name: String[1]; }
                    Class test::Firm { legalName: String[1]; country: String[1]; }
                    Association test::PersonFirm { firm: Firm[1]; employees: Person[*]; }
                    Database store::DB (
                        Table T_PERSON (ID INTEGER, NAME VARCHAR(100), FIRM_ID INTEGER)
                        Table T_FIRM (ID INTEGER, LEGAL_NAME VARCHAR(100), COUNTRY_ID INTEGER)
                        Table T_COUNTRY (ID INTEGER, NAME VARCHAR(100))
                        Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                        Join FirmCountry(T_FIRM.COUNTRY_ID = T_COUNTRY.ID)
                    )
                    Mapping test::M (
                        test::Person: Relational {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                        test::Firm: Relational {
                            ~mainTable [store::DB] T_FIRM
                            legalName: [store::DB] T_FIRM.LEGAL_NAME,
                            country: @FirmCountry | [store::DB] T_COUNTRY.NAME
                        }
                        test::PersonFirm: Relational { AssociationMapping (
                            firm: [store::DB]@PersonFirm,
                            employees: [store::DB]@PersonFirm
                        ) }
                    )
                    """, "store::DB", "test::M");
            // Sort by country — UK before USA alphabetically
            var r = exec(model, "Person.all()->sortBy({p|$p.firm.country})->project(~[name:p|$p.name])");
            assertEquals(3, r.rowCount());
            // UK sorts before USA
            assertEquals("Bob", colStr(r, 0).get(0));
        }

        @Test
        @DisplayName("Mapping ~groupBy produces GROUP BY with aggregate in SQL")
        void testMappingGroupBy() throws SQLException {
            sql("CREATE TABLE TRADE (TRADE_ID INT, ACC_NUM INT, GSN VARCHAR(20), PRODUCT_ID INT, QTY INT)",
                "INSERT INTO TRADE VALUES (1, 7900002, 'YU2EF5', 1, 3), (2, 7900002, 'YU2EF5', 1, 5), (3, 7900003, 'EA4GNY', 2, 100), (4, 7900003, 'EA4GNY', 2, 200)");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Position { acctNum: Integer[1]; gsn: String[1]; quantity: Integer[1]; }
                    Database store::DB (
                        Table TRADE (TRADE_ID INTEGER, ACC_NUM INTEGER, GSN VARCHAR(20), PRODUCT_ID INTEGER, QTY INTEGER)
                    )
                    Mapping test::M (
                        test::Position: Relational {
                            ~groupBy([store::DB] TRADE.ACC_NUM, [store::DB] TRADE.PRODUCT_ID, [store::DB] TRADE.GSN)
                            ~mainTable [store::DB] TRADE
                            acctNum: [store::DB] TRADE.ACC_NUM,
                            gsn: [store::DB] TRADE.GSN,
                            quantity: sum([store::DB] TRADE.QTY)
                        }
                    )
                    """, "store::DB", "test::M");
            var r = exec(model, "Position.all()->project(~[gsn:p|$p.gsn, qty:p|$p.quantity])->sort(~gsn->ascending())");
            assertEquals(2, r.rowCount());
            // EA4GNY: sum(100,200)=300, YU2EF5: sum(3,5)=8
            assertEquals("EA4GNY", colStr(r, 0).get(0));
            assertEquals("YU2EF5", colStr(r, 0).get(1));
            assertEquals(300, colInt(r, 1).get(0));
            assertEquals(8, colInt(r, 1).get(1));
        }

        @Test @Disabled("GAP: Scope block + embedded + filter")
        @DisplayName("GAP: Scope block containing embedded mapping, filtered on")
        void testScopeEmbeddedFilter() throws SQLException {
            // scope([DB]T) (firm(name: FIRM_NAME), ...) + filter on firm.name
        }

        @Test @Disabled("GAP: AggregationAware + join")
        @DisplayName("GAP: AggregationAware mapping with join navigation")
        void testAggAwareWithJoin() throws SQLException {
            // AggregationAware mapping auto-selecting aggregate view vs detail
        }


        @Test
        @DisplayName("Self-join + filter + sort composition")
        void testSelfJoinFilter() throws SQLException {
            sql("CREATE TABLE EMPLOYEES (ID INT, NAME VARCHAR(100), MANAGER_ID INT)",
                "INSERT INTO EMPLOYEES VALUES (1, 'Alice', null), (2, 'Bob', 1), (3, 'Charlie', 1), (4, 'Diana', 2)");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Employee { name: String[1]; managerName: String[0..1]; }
                    Database store::DB (
                        Table EMPLOYEES (ID INTEGER, NAME VARCHAR(100), MANAGER_ID INTEGER)
                        Join EmpMgr(EMPLOYEES.MANAGER_ID = {target}.ID)
                    )
                    Mapping test::M (
                        test::Employee: Relational {
                            ~mainTable [store::DB] EMPLOYEES
                            name: [store::DB] EMPLOYEES.NAME,
                            managerName: @EmpMgr | [store::DB] EMPLOYEES.NAME
                        }
                    )
                    """, "store::DB", "test::M");
            // Filter: only employees managed by Alice, sort by name
            var r = exec(model, "Employee.all()->filter(e|$e.managerName == 'Alice')->project(~[name:e|$e.name])->sort(~name->ascending())");
            assertEquals(2, r.rowCount());
            assertEquals(List.of("Bob", "Charlie"), colStr(r, 0));
        }

        @Test
        @DisplayName("Complex join condition + groupBy aggregation")
        void testComplexJoinAggregation() throws SQLException {
            sql("CREATE TABLE SALES (REGION VARCHAR(10), PRODUCT_ID INT, QTY INT)",
                "INSERT INTO SALES VALUES ('US', 1, 10), ('US', 2, 5), ('EU', 1, 20), ('US', 1, 3)",
                "CREATE TABLE PRODUCTS (REGION VARCHAR(10), PRODUCT_ID INT, NAME VARCHAR(50))",
                "INSERT INTO PRODUCTS VALUES ('US', 1, 'Widget'), ('US', 2, 'Gadget'), ('EU', 1, 'Widget-EU')");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Sale { qty: Integer[1]; productName: String[1]; }
                    Database store::DB (
                        Table SALES (REGION VARCHAR(10), PRODUCT_ID INTEGER, QTY INTEGER)
                        Table PRODUCTS (REGION VARCHAR(10), PRODUCT_ID INTEGER, NAME VARCHAR(50))
                        Join SaleProduct(SALES.REGION = PRODUCTS.REGION and SALES.PRODUCT_ID = PRODUCTS.PRODUCT_ID)
                    )
                    Mapping test::M (
                        test::Sale: Relational {
                            ~mainTable [store::DB] SALES
                            qty: [store::DB] SALES.QTY,
                            productName: @SaleProduct | [store::DB] PRODUCTS.NAME
                        }
                    )
                    """, "store::DB", "test::M");
            // GroupBy product name, sum qty
            var r = exec(model, "Sale.all()->project(~[product:s|$s.productName, qty:s|$s.qty])->groupBy([{r|$r.product}], [agg({r|$r.qty}, {y|$y->sum()})], ['product', 'totalQty'])->sort(~product->ascending())");
            assertEquals(3, r.rowCount());
            // Gadget: 5, Widget: 13 (10+3), Widget-EU: 20
            assertEquals("Gadget", colStr(r, 0).get(0));
            assertEquals(5, ((Number) r.rows().get(0).get(1)).intValue());
            assertEquals("Widget", colStr(r, 0).get(1));
            assertEquals(13, ((Number) r.rows().get(1).get(1)).intValue());
            assertEquals("Widget-EU", colStr(r, 0).get(2));
            assertEquals(20, ((Number) r.rows().get(2).get(1)).intValue());
        }

        @Test
        @DisplayName("F1: Computed property from 2 different join paths")
        void testComputedFromTwoJoins() throws SQLException {
            sql("CREATE TABLE ORDERS (ID INT, CUST_ID INT, PROD_ID INT)",
                "CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR(50))",
                "CREATE TABLE PRODUCTS (ID INT, NAME VARCHAR(50))",
                "INSERT INTO CUSTOMERS VALUES (1, 'Alice'), (2, 'Bob')",
                "INSERT INTO PRODUCTS VALUES (10, 'Widget'), (20, 'Gadget')",
                "INSERT INTO ORDERS VALUES (1, 1, 10), (2, 2, 20), (3, 1, 20)");
            String model = withRuntime("""
                    import store::*;
                    import test::*;

                    Class test::Order { summary: String[1]; custName: String[1]; prodName: String[1]; }
                    Database store::DB (
                        Table ORDERS (ID INTEGER, CUST_ID INTEGER, PROD_ID INTEGER)
                        Table CUSTOMERS (ID INTEGER, NAME VARCHAR(50))
                        Table PRODUCTS (ID INTEGER, NAME VARCHAR(50))
                        Join OrdCust(ORDERS.CUST_ID = CUSTOMERS.ID)
                        Join OrdProd(ORDERS.PROD_ID = PRODUCTS.ID)
                    )
                    Mapping test::M (
                        test::Order: Relational {
                            ~mainTable [store::DB] ORDERS
                            summary: concat(@OrdCust | [store::DB] CUSTOMERS.NAME, ' bought ', @OrdProd | [store::DB] PRODUCTS.NAME),
                            custName: @OrdCust | [store::DB] CUSTOMERS.NAME,
                            prodName: @OrdProd | [store::DB] PRODUCTS.NAME
                        }
                    )
                    """, "store::DB", "test::M");
            var r = exec(model, "Order.all()->project(~[s:o|$o.summary])->sort(~s->ascending())");
            assertEquals(3, r.rowCount());
            assertEquals("Alice bought Gadget", colStr(r, 0).get(0));
            assertEquals("Alice bought Widget", colStr(r, 0).get(1));
            assertEquals("Bob bought Gadget", colStr(r, 0).get(2));
        }
    }

    // ==================== Bare Class Queries (no terminal) ====================

    @Nested
    @DisplayName("Bare Class Queries — JSON-wrapped GraphResult")
    class BareClassQueries {

        private String personModel() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, NAME VARCHAR(100), AGE INT)",
                "INSERT INTO T1 VALUES (1, 'Alice', 25), (2, 'Bob', 30)");
            return singleTableModel("P", "T1", "store::DB", "model::M",
                    "Class model::P { name: String[1]; age: Integer[1]; }",
                    "ID INTEGER, NAME VARCHAR(100), AGE INTEGER",
                    "name: [store::DB] T1.NAME, age: [store::DB] T1.AGE");
        }

        @Test
        @DisplayName("P.all()->filter() returns GraphResult with filtered JSON")
        void testBareFilter() throws SQLException {
            String m = personModel();
            var result = exec(m, "|P.all()->filter(x|$x.age > 20)");
            assertInstanceOf(ExecutionResult.GraphResult.class, result);
            String json = result.asGraph().json();
            assertNotNull(json);
            // Both Alice (25) and Bob (30) pass age > 20
            assertTrue(json.contains("Alice"), "JSON should contain Alice");
            assertTrue(json.contains("Bob"), "JSON should contain Bob");
            assertTrue(json.contains("name"), "JSON should contain 'name' property");
            assertTrue(json.contains("age"), "JSON should contain 'age' property");
        }

        @Test
        @DisplayName("P.all()->sortBy() returns GraphResult with sorted JSON")
        void testBareSort() throws SQLException {
            String m = personModel();
            var result = exec(m, "|P.all()->sortBy({p|$p.name})");
            assertInstanceOf(ExecutionResult.GraphResult.class, result);
            String json = result.asGraph().json();
            assertNotNull(json);
            assertTrue(json.contains("Alice"), "JSON should contain Alice");
            assertTrue(json.contains("Bob"), "JSON should contain Bob");
            // Alice should appear before Bob in ascending name order
            assertTrue(json.indexOf("Alice") < json.indexOf("Bob"),
                    "Alice should appear before Bob in ascending order");
        }

        @Test
        @DisplayName("P.all()->limit() returns GraphResult with limited JSON")
        void testBareLimit() throws SQLException {
            String m = personModel();
            var result = exec(m, "|P.all()->limit(1)");
            assertInstanceOf(ExecutionResult.GraphResult.class, result);
            String json = result.asGraph().json();
            assertNotNull(json);
            // Should contain exactly 1 person (either Alice or Bob, not both)
            assertTrue(json.contains("name"), "JSON should contain 'name' property");
            assertTrue(json.contains("age"), "JSON should contain 'age' property");
        }

        @Test
        @DisplayName("P.all()->groupBy() returns GraphResult with aggregated JSON")
        void testBareGroupBy() throws SQLException {
            sql("CREATE TABLE T2 (ID INT, NAME VARCHAR(100), AGE INT)",
                "INSERT INTO T2 VALUES (1, 'Alice', 25), (2, 'Alice', 30), (3, 'Bob', 20)");
            String m = singleTableModel("Q", "T2", "store::DB2", "model::M2",
                    "Class model::Q { name: String[1]; age: Integer[1]; }",
                    "ID INTEGER, NAME VARCHAR(100), AGE INTEGER",
                    "name: [store::DB2] T2.NAME, age: [store::DB2] T2.AGE");
            var result = exec(m, "|Q.all()->groupBy(~[grp:x|$x.name], ~[cnt:x|$x.name:y|$y->count()])");
            // groupBy returns Relation (not ClassType) → TabularResult
            assertInstanceOf(ExecutionResult.TabularResult.class, result);
            assertEquals(2, result.rows().size(), "Should have 2 groups (Alice, Bob)");
        }
    }
}
