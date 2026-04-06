package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DynaFunction expressions in relational property mappings.
 *
 * Each test creates a minimal schema, defines a Pure model where one or more properties
 * use DynaFunction expressions, executes a query through the full pipeline
 * (Parser → Compiler → PlanGenerator → Dialect → DuckDB), and asserts on real results.
 *
 * Organized as:
 * - One test per DynaFunction (string, null, boolean, arithmetic, comparison, conditional, misc)
 * - Bonus composed tests (nested functions, multiple dyna properties, join chains, filters)
 */
@DisplayName("DynaFunction Property Mapping Integration Tests")
class DynaFunctionIntegrationTest extends AbstractDatabaseTest {

    private final QueryService qs = new QueryService();

    @Override
    protected SQLDialect getDialect() { return DuckDBDialect.INSTANCE; }

    @Override
    protected String getDatabaseType() { return "DuckDB"; }

    @Override
    protected String getJdbcUrl() { return "jdbc:duckdb:"; }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) connection.close();
    }

    // ==================== Helpers ====================

    private String withRuntime(String model, String dbName, String mappingName) {
        return model + """

                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    private ExecutionResult exec(String model, String query) throws SQLException {
        return qs.execute(model, query, "test::RT", connection);
    }

    private void sql(String... statements) throws SQLException {
        try (Statement s = connection.createStatement()) {
            for (String stmt : statements) s.execute(stmt);
        }
    }

    private List<String> colStr(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? row.get(idx).toString() : null)
                .collect(Collectors.toList());
    }

    private List<Integer> colInt(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? ((Number) row.get(idx)).intValue() : null)
                .collect(Collectors.toList());
    }

    private List<Boolean> colBool(ExecutionResult r, int idx) {
        return r.rows().stream().map(row -> row.get(idx) != null ? (Boolean) row.get(idx) : null)
                .collect(Collectors.toList());
    }

    private Object scalar(ExecutionResult r) {
        return r.rows().get(0).get(0);
    }

    // ==================== String DynaFunctions ====================

    @Nested
    @DisplayName("String DynaFunctions")
    class StringFunctions {

        @Test
        @DisplayName("concat() concatenates multiple columns and literals")
        void testConcat() throws SQLException {
            sql("CREATE TABLE NAMES (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50))",
                "INSERT INTO NAMES VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')");
            String model = withRuntime("""
                    Class model::P { fullName: String[1]; }
                    Database store::DB ( Table NAMES ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] NAMES fullName: concat([store::DB] NAMES.FIRST, ' ', [store::DB] NAMES.LAST) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.fullName], ['fullName'])");
            assertEquals(2, result.rows().size());
            assertTrue(colStr(result, 0).contains("John Doe"));
            assertTrue(colStr(result, 0).contains("Jane Smith"));
        }

        @Test
        @DisplayName("toLower() lowercases a column")
        void testToLower() throws SQLException {
            sql("CREATE TABLE T1 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T1 VALUES (1, 'ALICE'), (2, 'BOB')");
            String model = withRuntime("""
                    Class model::P { lowerName: String[1]; }
                    Database store::DB ( Table T1 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T1 lowerName: toLower([store::DB] T1.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.lowerName], ['lowerName'])");
            assertTrue(colStr(result, 0).contains("alice"));
            assertTrue(colStr(result, 0).contains("bob"));
        }

        @Test
        @DisplayName("toUpper() uppercases a column")
        void testToUpper() throws SQLException {
            sql("CREATE TABLE T2 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T2 VALUES (1, 'alice'), (2, 'bob')");
            String model = withRuntime("""
                    Class model::P { upperName: String[1]; }
                    Database store::DB ( Table T2 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T2 upperName: toUpper([store::DB] T2.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.upperName], ['upperName'])");
            assertTrue(colStr(result, 0).contains("ALICE"));
            assertTrue(colStr(result, 0).contains("BOB"));
        }

        @Test
        @DisplayName("trim() removes whitespace")
        void testTrim() throws SQLException {
            sql("CREATE TABLE T3 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T3 VALUES (1, '  hello  '), (2, ' world ')");
            String model = withRuntime("""
                    Class model::P { trimmed: String[1]; }
                    Database store::DB ( Table T3 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T3 trimmed: trim([store::DB] T3.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.trimmed], ['trimmed'])");
            assertTrue(colStr(result, 0).contains("hello"));
            assertTrue(colStr(result, 0).contains("world"));
        }

        @Test
        @DisplayName("substring() extracts a substring (Pure 0-based)")
        void testSubstring() throws SQLException {
            sql("CREATE TABLE CODES (ID INT, CODE VARCHAR(10))",
                "INSERT INTO CODES VALUES (1, 'US-CA-01'), (2, 'UK-LN-02')");
            String model = withRuntime("""
                    Class model::C { country: String[1]; }
                    Database store::DB ( Table CODES ( ID INTEGER, CODE VARCHAR(10) ) )
                    Mapping model::M ( C: Relational { ~mainTable [store::DB] CODES country: substring([store::DB] CODES.CODE, 0, 2) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "C.all()->project([x|$x.country], ['country'])");
            assertTrue(colStr(result, 0).contains("US"));
            assertTrue(colStr(result, 0).contains("UK"));
        }

        @Test
        @DisplayName("replace() substitutes text")
        void testReplace() throws SQLException {
            sql("CREATE TABLE T4 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T4 VALUES (1, 'foo-bar'), (2, 'foo-baz')");
            String model = withRuntime("""
                    Class model::P { fixed: String[1]; }
                    Database store::DB ( Table T4 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T4 fixed: replace([store::DB] T4.NAME, 'foo', 'qux') } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.fixed], ['fixed'])");
            assertTrue(colStr(result, 0).contains("qux-bar"));
            assertTrue(colStr(result, 0).contains("qux-baz"));
        }

        @Test
        @DisplayName("length() returns string length")
        void testLength() throws SQLException {
            sql("CREATE TABLE T5 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T5 VALUES (1, 'hi'), (2, 'hello')");
            String model = withRuntime("""
                    Class model::P { len: Integer[1]; }
                    Database store::DB ( Table T5 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T5 len: length([store::DB] T5.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.len], ['len'])");
            var lens = colInt(result, 0);
            assertTrue(lens.contains(2));
            assertTrue(lens.contains(5));
        }

        @Test
        @DisplayName("repeatString() repeats a string N times")
        void testRepeatString() throws SQLException {
            sql("CREATE TABLE T6 (ID INT, NAME VARCHAR(10))",
                "INSERT INTO T6 VALUES (1, 'ab'), (2, 'x')");
            String model = withRuntime("""
                    Class model::P { repeated: String[1]; }
                    Database store::DB ( Table T6 ( ID INTEGER, NAME VARCHAR(10) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T6 repeated: repeatString([store::DB] T6.NAME, 3) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.repeated], ['repeated'])");
            assertTrue(colStr(result, 0).contains("ababab"));
            assertTrue(colStr(result, 0).contains("xxx"));
        }

        @Test
        @DisplayName("md5() hashes a string")
        void testMd5() throws SQLException {
            sql("CREATE TABLE T7 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T7 VALUES (1, 'hello')");
            String model = withRuntime("""
                    Class model::P { hash: String[1]; }
                    Database store::DB ( Table T7 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T7 hash: md5([store::DB] T7.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.hash], ['hash'])");
            assertEquals(1, result.rows().size());
            assertEquals("5d41402abc4b2a76b9719d911017c592", scalar(result));
        }

        @Test
        @DisplayName("sha256() hashes a string with SHA-256")
        void testSha256() throws SQLException {
            sql("CREATE TABLE T8 (ID INT, NAME VARCHAR(50))",
                "INSERT INTO T8 VALUES (1, 'hello')");
            String model = withRuntime("""
                    Class model::P { hash: String[1]; }
                    Database store::DB ( Table T8 ( ID INTEGER, NAME VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] T8 hash: sha256([store::DB] T8.NAME) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.hash], ['hash'])");
            assertEquals(1, result.rows().size());
            assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", scalar(result));
        }
    }

    // ==================== Null DynaFunctions ====================

    @Nested
    @DisplayName("Null DynaFunctions")
    class NullFunctions {

        @Test
        @DisplayName("isNull() returns true when column IS NULL")
        void testIsNull() throws SQLException {
            sql("CREATE TABLE TN1 (ID INT, VAL VARCHAR(50))",
                "INSERT INTO TN1 VALUES (1, 'present'), (2, NULL)");
            String model = withRuntime("""
                    Class model::P { missing: Boolean[1]; }
                    Database store::DB ( Table TN1 ( ID INTEGER, VAL VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] TN1 missing: isNull([store::DB] TN1.VAL) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.missing], ['missing'])");
            var vals = colBool(result, 0);
            assertTrue(vals.contains(true));
            assertTrue(vals.contains(false));
        }

        @Test
        @DisplayName("isNotNull() returns true when column IS NOT NULL")
        void testIsNotNull() throws SQLException {
            sql("CREATE TABLE TN2 (ID INT, VAL VARCHAR(50))",
                "INSERT INTO TN2 VALUES (1, 'present'), (2, NULL)");
            String model = withRuntime("""
                    Class model::P { present: Boolean[1]; }
                    Database store::DB ( Table TN2 ( ID INTEGER, VAL VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] TN2 present: isNotNull([store::DB] TN2.VAL) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.present], ['present'])");
            var vals = colBool(result, 0);
            assertTrue(vals.contains(true));
            assertTrue(vals.contains(false));
        }
    }

    // ==================== Boolean DynaFunctions ====================

    @Nested
    @DisplayName("Boolean DynaFunctions")
    class BooleanFunctions {

        @Test
        @DisplayName("sqlTrue() returns constant true")
        void testSqlTrue() throws SQLException {
            sql("CREATE TABLE TB1 (ID INT)",
                "INSERT INTO TB1 VALUES (1), (2)");
            String model = withRuntime("""
                    Class model::P { flag: Boolean[1]; }
                    Database store::DB ( Table TB1 ( ID INTEGER ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] TB1 flag: sqlTrue() } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.flag], ['flag'])");
            assertEquals(2, result.rows().size());
            assertTrue(colBool(result, 0).stream().allMatch(b -> b));
        }

        @Test
        @DisplayName("sqlFalse() returns constant false")
        void testSqlFalse() throws SQLException {
            sql("CREATE TABLE TB2 (ID INT)",
                "INSERT INTO TB2 VALUES (1), (2)");
            String model = withRuntime("""
                    Class model::P { flag: Boolean[1]; }
                    Database store::DB ( Table TB2 ( ID INTEGER ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] TB2 flag: sqlFalse() } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.flag], ['flag'])");
            assertEquals(2, result.rows().size());
            assertTrue(colBool(result, 0).stream().noneMatch(b -> b));
        }
    }

    // ==================== Arithmetic DynaFunctions ====================

    @Nested
    @DisplayName("Arithmetic DynaFunctions")
    class ArithmeticFunctions {

        @Test
        @DisplayName("plus() adds two columns")
        void testPlus() throws SQLException {
            sql("CREATE TABLE TA1 (ID INT, A INT, B INT)",
                "INSERT INTO TA1 VALUES (1, 10, 20), (2, 30, 40)");
            String model = withRuntime("""
                    Class model::R { total: Integer[1]; }
                    Database store::DB ( Table TA1 ( ID INTEGER, A INTEGER, B INTEGER ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TA1 total: plus([store::DB] TA1.A, [store::DB] TA1.B) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.total], ['total'])");
            var totals = colInt(result, 0);
            assertTrue(totals.contains(30));
            assertTrue(totals.contains(70));
        }

        @Test
        @DisplayName("sub() subtracts two columns")
        void testSub() throws SQLException {
            sql("CREATE TABLE TA2 (ID INT, A INT, B INT)",
                "INSERT INTO TA2 VALUES (1, 50, 20), (2, 100, 30)");
            String model = withRuntime("""
                    Class model::R { diff: Integer[1]; }
                    Database store::DB ( Table TA2 ( ID INTEGER, A INTEGER, B INTEGER ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TA2 diff: sub([store::DB] TA2.A, [store::DB] TA2.B) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.diff], ['diff'])");
            var diffs = colInt(result, 0);
            assertTrue(diffs.contains(30));
            assertTrue(diffs.contains(70));
        }

        @Test
        @DisplayName("divideRound() divides and rounds to scale")
        void testDivideRound() throws SQLException {
            sql("CREATE TABLE TA3 (ID INT, A INT, B INT)",
                "INSERT INTO TA3 VALUES (1, 10, 3), (2, 22, 7)");
            String model = withRuntime("""
                    Class model::R { ratio: Float[1]; }
                    Database store::DB ( Table TA3 ( ID INTEGER, A INTEGER, B INTEGER ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TA3 ratio: divideRound([store::DB] TA3.A, [store::DB] TA3.B, 2) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.ratio], ['ratio'])");
            assertEquals(2, result.rows().size());
            // 10/3 = 3.33, 22/7 = 3.14
            var vals = result.rows().stream()
                    .map(row -> ((Number) row.get(0)).doubleValue())
                    .collect(Collectors.toList());
            assertTrue(vals.stream().anyMatch(v -> Math.abs(v - 3.33) < 0.01));
            assertTrue(vals.stream().anyMatch(v -> Math.abs(v - 3.14) < 0.01));
        }
    }

    // ==================== Comparison DynaFunctions ====================

    @Nested
    @DisplayName("Comparison DynaFunctions")
    class ComparisonFunctions {

        @Test
        @DisplayName("notEqualAnsi() compares with <>")
        void testNotEqualAnsi() throws SQLException {
            sql("CREATE TABLE TC1 (ID INT, A VARCHAR(10), B VARCHAR(10))",
                "INSERT INTO TC1 VALUES (1, 'x', 'x'), (2, 'x', 'y')");
            String model = withRuntime("""
                    Class model::R { different: Boolean[1]; }
                    Database store::DB ( Table TC1 ( ID INTEGER, A VARCHAR(10), B VARCHAR(10) ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TC1 different: notEqualAnsi([store::DB] TC1.A, [store::DB] TC1.B) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.different], ['different'])");
            var vals = colBool(result, 0);
            assertTrue(vals.contains(true));
            assertTrue(vals.contains(false));
        }

        @Test
        @DisplayName("isDistinct() uses IS DISTINCT FROM")
        void testIsDistinct() throws SQLException {
            sql("CREATE TABLE TC2 (ID INT, A VARCHAR(10), B VARCHAR(10))",
                "INSERT INTO TC2 VALUES (1, 'x', 'x'), (2, 'x', NULL), (3, NULL, NULL)");
            String model = withRuntime("""
                    Class model::R { distinct: Boolean[1]; }
                    Database store::DB ( Table TC2 ( ID INTEGER, A VARCHAR(10), B VARCHAR(10) ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TC2 distinct: isDistinct([store::DB] TC2.A, [store::DB] TC2.B) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.distinct], ['distinct'])");
            // row 1: 'x' vs 'x' → not distinct (false)
            // row 2: 'x' vs NULL → distinct (true)
            // row 3: NULL vs NULL → not distinct (false)
            var vals = colBool(result, 0);
            assertEquals(3, vals.size());
            assertEquals(1, vals.stream().filter(b -> b).count()); // only row 2 is distinct
        }
    }

    // ==================== Conditional DynaFunctions ====================

    @Nested
    @DisplayName("Conditional DynaFunctions")
    class ConditionalFunctions {

        @Test
        @DisplayName("if(equal(...)) maps codes to labels")
        void testIfEqual() throws SQLException {
            sql("CREATE TABLE STATUS (ID INT, CODE VARCHAR(10))",
                "INSERT INTO STATUS VALUES (1, 'A'), (2, 'I')");
            String model = withRuntime("""
                    Class model::R { label: String[1]; }
                    Database store::DB ( Table STATUS ( ID INTEGER, CODE VARCHAR(10) ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] STATUS label: if(equal([store::DB] STATUS.CODE, 'A'), 'Active', 'Inactive') } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.label], ['label'])");
            var labels = colStr(result, 0);
            assertTrue(labels.contains("Active"));
            assertTrue(labels.contains("Inactive"));
        }
    }

    // ==================== Misc DynaFunctions ====================

    @Nested
    @DisplayName("Misc DynaFunctions")
    class MiscFunctions {

        @Test
        @DisplayName("group() passes through value (parenthesization)")
        void testGroup() throws SQLException {
            sql("CREATE TABLE TM1 (ID INT, VAL INT)",
                "INSERT INTO TM1 VALUES (1, 42), (2, 99)");
            String model = withRuntime("""
                    Class model::R { val: Integer[1]; }
                    Database store::DB ( Table TM1 ( ID INTEGER, VAL INTEGER ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TM1 val: group([store::DB] TM1.VAL) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.val], ['val'])");
            var vals = colInt(result, 0);
            assertTrue(vals.contains(42));
            assertTrue(vals.contains(99));
        }

        @Test
        @DisplayName("currentUserId() returns a non-null string")
        void testCurrentUserId() throws SQLException {
            sql("CREATE TABLE TM2 (ID INT)",
                "INSERT INTO TM2 VALUES (1)");
            String model = withRuntime("""
                    Class model::R { userId: String[1]; }
                    Database store::DB ( Table TM2 ( ID INTEGER ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] TM2 userId: currentUserId() } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.userId], ['userId'])");
            assertEquals(1, result.rows().size());
            assertNotNull(scalar(result));
        }
    }

    // ==================== Bonus: Composition ====================

    @Nested
    @DisplayName("Composed DynaFunctions")
    class ComposedFunctions {

        @Test
        @DisplayName("Nested: toLower(concat(FIRST, LAST))")
        void testNestedToLowerConcat() throws SQLException {
            sql("CREATE TABLE NAMES (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50))",
                "INSERT INTO NAMES VALUES (1, 'John', 'DOE')");
            String model = withRuntime("""
                    Class model::P { email: String[1]; }
                    Database store::DB ( Table NAMES ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50) ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] NAMES email: toLower(concat([store::DB] NAMES.FIRST, [store::DB] NAMES.LAST)) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.email], ['email'])");
            assertEquals("johndoe", scalar(result));
        }

        @Test
        @DisplayName("Multiple dyna properties on same class")
        void testMultipleDynaProperties() throws SQLException {
            sql("CREATE TABLE EMPS (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), SALARY INT, BONUS INT)",
                "INSERT INTO EMPS VALUES (1, 'Alice', 'Wonder', 100, 20), (2, 'Bob', 'Builder', 200, 30)");
            String model = withRuntime("""
                    Class model::E { fullName: String[1]; totalComp: Integer[1]; upperLast: String[1]; }
                    Database store::DB ( Table EMPS ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), SALARY INTEGER, BONUS INTEGER ) )
                    Mapping model::M ( E: Relational { ~mainTable [store::DB] EMPS
                        fullName: concat([store::DB] EMPS.FIRST, ' ', [store::DB] EMPS.LAST),
                        totalComp: plus([store::DB] EMPS.SALARY, [store::DB] EMPS.BONUS),
                        upperLast: toUpper([store::DB] EMPS.LAST)
                    } )
                    """, "store::DB", "model::M");
            var result = exec(model, "E.all()->project([x|$x.fullName, x|$x.totalComp, x|$x.upperLast], ['name', 'comp', 'upper'])");
            assertEquals(2, result.rows().size());
            assertTrue(colStr(result, 0).contains("Alice Wonder"));
            assertTrue(colInt(result, 1).contains(120));
            assertTrue(colStr(result, 2).contains("WONDER"));
        }

        @Test
        @DisplayName("DynaFunction property used in Pure filter")
        void testDynaPropertyFilteredOn() throws SQLException {
            sql("CREATE TABLE ITEMS (ID INT, NAME VARCHAR(50), PRICE INT, QTY INT)",
                "INSERT INTO ITEMS VALUES (1, 'Widget', 10, 5), (2, 'Gadget', 20, 3), (3, 'Doohickey', 5, 100)");
            String model = withRuntime("""
                    Class model::I { lowerName: String[1]; total: Integer[1]; }
                    Database store::DB ( Table ITEMS ( ID INTEGER, NAME VARCHAR(50), PRICE INTEGER, QTY INTEGER ) )
                    Mapping model::M ( I: Relational { ~mainTable [store::DB] ITEMS
                        lowerName: toLower([store::DB] ITEMS.NAME),
                        total: plus([store::DB] ITEMS.PRICE, [store::DB] ITEMS.QTY)
                    } )
                    """, "store::DB", "model::M");
            // Filter on total > 20
            var result = exec(model, "I.all()->filter(x|$x.total > 20)->project([x|$x.lowerName], ['name'])");
            assertEquals(2, result.rows().size());
            var names = colStr(result, 0);
            assertTrue(names.contains("gadget"));   // 20+3=23
            assertTrue(names.contains("doohickey")); // 5+100=105
        }

        @Test
        @DisplayName("Arithmetic composition: sub(plus(A, B), C)")
        void testArithmeticComposition() throws SQLException {
            sql("CREATE TABLE CALC (ID INT, A INT, B INT, C INT)",
                "INSERT INTO CALC VALUES (1, 10, 20, 5), (2, 100, 50, 25)");
            String model = withRuntime("""
                    Class model::R { result: Integer[1]; }
                    Database store::DB ( Table CALC ( ID INTEGER, A INTEGER, B INTEGER, C INTEGER ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] CALC result: sub(plus([store::DB] CALC.A, [store::DB] CALC.B), [store::DB] CALC.C) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.result], ['result'])");
            var vals = colInt(result, 0);
            assertTrue(vals.contains(25));   // (10+20)-5
            assertTrue(vals.contains(125));  // (100+50)-25
        }

        @Test
        @DisplayName("if + string composition: if(equal(CODE,'A'), toLower(NAME), toUpper(NAME))")
        void testIfWithStringFunctions() throws SQLException {
            sql("CREATE TABLE MIX (ID INT, CODE VARCHAR(5), NAME VARCHAR(50))",
                "INSERT INTO MIX VALUES (1, 'A', 'Hello'), (2, 'B', 'World')");
            String model = withRuntime("""
                    Class model::R { display: String[1]; }
                    Database store::DB ( Table MIX ( ID INTEGER, CODE VARCHAR(5), NAME VARCHAR(50) ) )
                    Mapping model::M ( R: Relational { ~mainTable [store::DB] MIX display: if(equal([store::DB] MIX.CODE, 'A'), toLower([store::DB] MIX.NAME), toUpper([store::DB] MIX.NAME)) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "R.all()->project([x|$x.display], ['display'])");
            var vals = colStr(result, 0);
            assertTrue(vals.contains("hello"));  // CODE='A' → toLower
            assertTrue(vals.contains("WORLD"));  // CODE='B' → toUpper
        }

        @Test @Disabled("GAP: JoinNavigation inside DynaFunction args needs JOIN + scalar cross-table reference")
        @DisplayName("DynaFunction with join chain: concat with joined column")
        void testDynaFunctionWithJoinChain() throws SQLException {
            sql("CREATE TABLE DEPT (ID INT, DEPT_NAME VARCHAR(50))",
                "INSERT INTO DEPT VALUES (1, 'Engineering'), (2, 'Sales')",
                "CREATE TABLE EMP (ID INT, NAME VARCHAR(50), DEPT_ID INT)",
                "INSERT INTO EMP VALUES (1, 'Alice', 1), (2, 'Bob', 2)");
            String model = withRuntime("""
                    Class model::E { label: String[1]; }
                    Database store::DB (
                        Table EMP ( ID INTEGER, NAME VARCHAR(50), DEPT_ID INTEGER )
                        Table DEPT ( ID INTEGER, DEPT_NAME VARCHAR(50) )
                        Join EmpDept(EMP.DEPT_ID = DEPT.ID)
                    )
                    Mapping model::M ( E: Relational { ~mainTable [store::DB] EMP
                        label: concat([store::DB] EMP.NAME, ' - ', @EmpDept | [store::DB] DEPT.DEPT_NAME)
                    } )
                    """, "store::DB", "model::M");
            var result = exec(model, "E.all()->project([x|$x.label], ['label'])");
            var labels = colStr(result, 0);
            assertTrue(labels.contains("Alice - Engineering"));
            assertTrue(labels.contains("Bob - Sales"));
        }

        @Test
        @DisplayName("DynaFunction property + sort + limit")
        void testDynaPropertyWithSortAndLimit() throws SQLException {
            sql("CREATE TABLE WORDS (ID INT, WORD VARCHAR(50))",
                "INSERT INTO WORDS VALUES (1, 'banana'), (2, 'apple'), (3, 'cherry')");
            String model = withRuntime("""
                    Class model::W { upper: String[1]; }
                    Database store::DB ( Table WORDS ( ID INTEGER, WORD VARCHAR(50) ) )
                    Mapping model::M ( W: Relational { ~mainTable [store::DB] WORDS upper: toUpper([store::DB] WORDS.WORD) } )
                    """, "store::DB", "model::M");
            var result = exec(model, "W.all()->sortBy(x|$x.upper)->limit(2)->project([x|$x.upper], ['upper'])");
            assertEquals(2, result.rows().size());
            assertEquals("APPLE", result.rows().get(0).get(0));
            assertEquals("BANANA", result.rows().get(1).get(0));
        }

        @Test
        @DisplayName("Mixed: dyna column + plain column on same mapping")
        void testMixedDynaAndPlainColumns() throws SQLException {
            sql("CREATE TABLE PEOPLE (ID INT, FIRST VARCHAR(50), LAST VARCHAR(50), AGE INT)",
                "INSERT INTO PEOPLE VALUES (1, 'John', 'Doe', 30), (2, 'Jane', 'Smith', 25)");
            String model = withRuntime("""
                    Class model::P { fullName: String[1]; age: Integer[1]; }
                    Database store::DB ( Table PEOPLE ( ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), AGE INTEGER ) )
                    Mapping model::M ( P: Relational { ~mainTable [store::DB] PEOPLE
                        fullName: concat([store::DB] PEOPLE.FIRST, ' ', [store::DB] PEOPLE.LAST),
                        age: [store::DB] PEOPLE.AGE
                    } )
                    """, "store::DB", "model::M");
            var result = exec(model, "P.all()->project([x|$x.fullName, x|$x.age], ['name', 'age'])");
            assertEquals(2, result.rows().size());
            assertTrue(colStr(result, 0).contains("John Doe"));
            assertTrue(colInt(result, 1).contains(30));
        }
    }
}
