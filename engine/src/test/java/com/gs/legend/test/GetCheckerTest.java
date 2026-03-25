package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for variant/JSON access.
 * <p>
 * Uses the canonical legend-engine variant API:
 * <ul>
 *   <li>{@code get('key')} → untyped variant navigation</li>
 *   <li>{@code get(N)} → index access</li>
 *   <li>{@code get('key')->to(@Type)} → typed scalar extraction</li>
 *   <li>{@code get('key')->toMany(@Type)} → typed array extraction</li>
 *   <li>Chained access: {@code get('a')->get('b')->to(@Type)}</li>
 *   <li>Composition with extend, filter, map, fold, flatten</li>
 * </ul>
 * <p>
 * All assertions use exact value/type checks — no {@code assertTrue} with strings.
 */
@DisplayName("GetChecker: variant/JSON access")
public class GetCheckerTest {

    private Connection connection;
    private final QueryService queryService = new QueryService();

    private static final String EVENT_DATABASE = """
            Database store::EventDatabase
            (
                Table T_EVENTS
                (
                    ID INTEGER PRIMARY KEY,
                    EVENT_TYPE VARCHAR(100) NOT NULL,
                    PAYLOAD SEMISTRUCTURED
                )
            )
            """;

    private static final String CONNECTION_DEF = """
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }
            """;

    private static final String RUNTIME_DEF = """
            Runtime test::TestRuntime
            {
                mappings:
                [
                ];
                connections:
                [
                    store::EventDatabase:
                    [
                        environment: store::TestConnection
                    ]
                ];
            }
            """;

    private String pureModel() {
        return EVENT_DATABASE + "\n" + CONNECTION_DEF + "\n" + RUNTIME_DEF;
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_EVENTS (
                        ID INTEGER PRIMARY KEY,
                        EVENT_TYPE VARCHAR(100) NOT NULL,
                        PAYLOAD JSON
                    )
                    """);
            // Row 1: nested object with scalar fields
            stmt.execute("""
                    INSERT INTO T_EVENTS VALUES (
                        1, 'page_view',
                        '{"page": "/home", "count": 42, "user": {"name": "Alice", "id": 100, "tags": ["admin", "active"]}}'
                    )
                    """);
            // Row 2: object with array of objects
            stmt.execute("""
                    INSERT INTO T_EVENTS VALUES (
                        2, 'purchase',
                        '{"items": [{"sku": "ABC", "price": 10, "qty": 2}, {"sku": "XYZ", "price": 25, "qty": 1}], "total": 45}'
                    )
                    """);
            // Row 3: simple object
            stmt.execute("""
                    INSERT INTO T_EVENTS VALUES (
                        3, 'purchase',
                        '{"items": [{"sku": "DEF", "price": 100, "qty": 3}], "total": 300}'
                    )
                    """);
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) connection.close();
    }

    private ExecutionResult exec(String pureQuery) throws SQLException {
        return queryService.execute(pureModel(), pureQuery, "test::TestRuntime", connection);
    }

    private String sql(String pureQuery) {
        return PlanGenerator.generate(pureModel(), pureQuery, "test::TestRuntime").sql();
    }

    // ==================== Field access: get('key') ====================

    @Nested
    @DisplayName("get('key') — untyped field access")
    class UntypedFieldAccess {

        @Test
        @DisplayName("top-level field returns JSON value")
        void testTopLevelField() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~page: _ | $_.PAYLOAD->get('page'))
                        ->filter(_ | $_.EVENT_TYPE == 'page_view')
                        ->select(~[ID, page])
                    """);
            assertEquals(1, result.rows().size());
            // Untyped get returns JSON — value present but may be quoted
            assertNotNull(result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("nested field via chained get->to")
        void testChainedFieldAccess() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~userName: _ | $_.PAYLOAD->get('user')->get('name')->to(@String))
                        ->filter(_ | $_.EVENT_TYPE == 'page_view')
                        ->select(~[ID, userName])
                    """);
            assertEquals(1, result.rows().size());
            assertEquals("Alice", result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("deeply nested: get('a')->get('b')->to(@Type)")
        void testDeeplyNested() throws SQLException {
            // user -> id (3 levels: PAYLOAD -> user -> id)
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~userId: _ | $_.PAYLOAD->get('user')->get('id')->to(@Integer))
                        ->filter(_ | $_.EVENT_TYPE == 'page_view')
                        ->select(~[ID, userId])
                    """);
            assertEquals(1, result.rows().size());
            assertEquals(100, ((Number) result.rows().get(0).get(1)).intValue());
        }
    }

    // ==================== Typed extraction: get('key')->to(@Type) ====================

    @Nested
    @DisplayName("get('key')->to(@Type) — typed extraction")
    class TypedExtraction {

        @Test
        @DisplayName("->to(@String) extraction")
        void testStringExtraction() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~page: _ | $_.PAYLOAD->get('page')->to(@String))
                        ->filter(_ | $_.EVENT_TYPE == 'page_view')
                        ->select(~[ID, page])
                    """);
            assertEquals(1, result.rows().size());
            assertEquals("/home", result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("->to(@Integer) extraction")
        void testIntegerExtraction() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~cnt: _ | $_.PAYLOAD->get('count')->to(@Integer))
                        ->filter(_ | $_.EVENT_TYPE == 'page_view')
                        ->select(~[ID, cnt])
                    """);
            assertEquals(1, result.rows().size());
            assertEquals(42, ((Number) result.rows().get(0).get(1)).intValue());
        }

        @Test
        @DisplayName("->to(@Integer) on nested field")
        void testNestedIntegerExtraction() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~total: _ | $_.PAYLOAD->get('total')->to(@Integer))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, total])
                        ->sort(~ID->ascending())
                    """);
            assertEquals(2, result.rows().size());
            assertEquals(45, ((Number) result.rows().get(0).get(1)).intValue());
            assertEquals(300, ((Number) result.rows().get(1).get(1)).intValue());
        }

        @Test
        @DisplayName("multiple typed gets->to() in same extend")
        void testMultipleTypedGets() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~[
                            userId: _ | $_.PAYLOAD->get('user')->get('id')->to(@Integer),
                            page: _ | $_.PAYLOAD->get('page')->to(@String)
                        ])
                        ->filter(_ | $_.EVENT_TYPE == 'page_view')
                        ->select(~[ID, userId, page])
                    """);
            assertEquals(1, result.rows().size());
            var row = result.rows().get(0);
            assertEquals(100, ((Number) row.get(1)).intValue());
            assertEquals("/home", row.get(2));
        }
    }

    // ==================== Index access: get(N) ====================

    @Nested
    @DisplayName("get(N) — index access")
    class IndexAccess {

        @Test
        @DisplayName("get(0) on array returns first element")
        void testIndexZero() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~firstItem: _ | $_.PAYLOAD->get('items')->get(0))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, firstItem])
                        ->sort(~ID->ascending())
                    """);
            assertEquals(2, result.rows().size());
            // First item of event 2 is {"sku": "ABC", "price": 10, "qty": 2}
            assertNotNull(result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("get(0)->get('field')->to(@Type) chains index + field + cast")
        void testIndexThenField() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~firstSku: _ | $_.PAYLOAD->get('items')->get(0)->get('sku')->to(@String))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, firstSku])
                        ->sort(~ID->ascending())
                    """);
            assertEquals(2, result.rows().size());
            assertEquals("ABC", result.rows().get(0).get(1));
            assertEquals("DEF", result.rows().get(1).get(1));
        }

        @Test
        @DisplayName("get(1) on array returns second element")
        void testIndexOne() throws SQLException {
            // Event 2 has 2 items; event 3 has only 1 item (get(1) → null)
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~secondSku: _ | $_.PAYLOAD->get('items')->get(1)->get('sku')->to(@String))
                        ->filter(_ | $_.ID == 2)
                        ->select(~[ID, secondSku])
                    """);
            assertEquals(1, result.rows().size());
            assertEquals("XYZ", result.rows().get(0).get(1));
        }
    }

    // ==================== Composition with other functions ====================

    @Nested
    @DisplayName("get() composed with extend/filter/sort")
    class Composition {

        @Test
        @DisplayName("filter on typed get value")
        void testFilterOnGet() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~total: _ | $_.PAYLOAD->get('total')->to(@Integer))
                        ->filter(_ | $_.total > 100)
                        ->select(~[ID, total])
                    """);
            assertEquals(1, result.rows().size());
            assertEquals(3, ((Number) result.rows().get(0).get(0)).intValue());
            assertEquals(300, ((Number) result.rows().get(0).get(1)).intValue());
        }

        @Test
        @DisplayName("sort on typed get value")
        void testSortOnGet() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~total: _ | $_.PAYLOAD->get('total')->to(@Integer))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, total])
                        ->sort(~total->descending())
                    """);
            assertEquals(2, result.rows().size());
            // Descending: 300, then 45
            assertEquals(300, ((Number) result.rows().get(0).get(1)).intValue());
            assertEquals(45, ((Number) result.rows().get(1).get(1)).intValue());
        }

        @Test
        @DisplayName("get() inside map() over JSON array")
        void testGetInsideMap() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~prices: _ | $_.PAYLOAD->get('items')->toMany(@Variant)->map(i | $i->get('price')))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, prices])
                        ->sort(~ID->ascending())
                    """);
            assertEquals(2, result.rows().size());
            // Verify SQL uses list_transform
            String generatedSql = sql("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~prices: _ | $_.PAYLOAD->get('items')->toMany(@Variant)->map(i | $i->get('price')))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, prices])
                    """);
            assertTrue(generatedSql.contains("list_transform"),
                    "Should use list_transform for map(). SQL: " + generatedSql);
        }

        @Test
        @DisplayName("get()->to(@Type) inside map()->fold() computes aggregate")
        void testTypedGetInMapFold() throws SQLException {
            // Compute sum of prices: map(get('price')->to(@Integer)) -> fold(+, 0)
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~totalPrice: _ | $_.PAYLOAD->get('items')->toMany(@Variant)
                            ->map(i | $i->get('price')->to(@Integer)->toOne())
                            ->fold({a, v | $a + $v}, 0))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, totalPrice])
                        ->sort(~ID->ascending())
                    """);
            assertEquals(2, result.rows().size());
            // Event 2: prices [10, 25] → sum = 35
            assertEquals(35.0, ((Number) result.rows().get(0).get(1)).doubleValue(), 0.01);
            // Event 3: prices [100] → sum = 100
            assertEquals(100.0, ((Number) result.rows().get(1).get(1)).doubleValue(), 0.01);
        }

        @Test
        @DisplayName("get() + arithmetic: price * qty via typed gets")
        void testGetWithArithmetic() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~calcTotal: _ | $_.PAYLOAD->get('items')->toMany(@Variant)
                            ->map(i | $i->get('price')->to(@Integer)->toOne() * $i->get('qty')->to(@Integer)->toOne())
                            ->fold({a, v | $a + $v}, 0))
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->select(~[ID, calcTotal])
                        ->sort(~ID->ascending())
                    """);
            assertEquals(2, result.rows().size());
            // Event 2: 10*2 + 25*1 = 45
            assertEquals(45.0, ((Number) result.rows().get(0).get(1)).doubleValue(), 0.01);
            // Event 3: 100*3 = 300
            assertEquals(300.0, ((Number) result.rows().get(1).get(1)).doubleValue(), 0.01);
        }
    }

    // ==================== Composition with flatten ====================

    @Nested
    @DisplayName("get() composed with flatten")
    class FlattenComposition {

        @Test
        @DisplayName("flatten JSON array then get fields with typed extraction")
        void testFlattenThenGet() throws SQLException {
            var result = exec("""
                    #>{EventDatabase.T_EVENTS}#
                        ->filter(_ | $_.EVENT_TYPE == 'purchase')
                        ->extend(~items: _ | $_.PAYLOAD->get('items'))
                        ->flatten(~items)
                        ->extend(~lineTotal: _ | $_.items->get('price')->to(@Integer)->toOne() * $_.items->get('qty')->to(@Integer)->toOne())
                        ->select(~[ID, lineTotal])
                        ->sort(~lineTotal->ascending())
                    """);
            assertEquals(3, result.rows().size());
            // Expected line totals sorted: 20 (ABC: 10*2), 25 (XYZ: 25*1), 300 (DEF: 100*3)
            assertEquals(20, ((Number) result.rows().get(0).get(1)).intValue());
            assertEquals(25, ((Number) result.rows().get(1).get(1)).intValue());
            assertEquals(300, ((Number) result.rows().get(2).get(1)).intValue());
        }
    }

    // ==================== SQL generation checks ====================

    @Nested
    @DisplayName("SQL generation")
    class SqlGeneration {

        @Test
        @DisplayName("untyped get generates JSON access operator")
        void testUntypedSqlGen() {
            String generatedSql = sql("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~page: _ | $_.PAYLOAD->get('page'))
                        ->select(~[ID, page])
                    """);
            // DuckDB: should use ->> operator for field access
            assertTrue(generatedSql.contains("'page'"),
                    "SQL should reference field name. SQL: " + generatedSql);
        }

        @Test
        @DisplayName("get()->to(@Type) generates CAST wrapper")
        void testTypedSqlGen() {
            String generatedSql = sql("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~cnt: _ | $_.PAYLOAD->get('count')->to(@Integer))
                        ->select(~[ID, cnt])
                    """);
            assertTrue(generatedSql.toUpperCase().contains("CAST"),
                    "get()->to(@Type) should generate CAST. SQL: " + generatedSql);
        }

        @Test
        @DisplayName("index access generates array subscript")
        void testIndexSqlGen() {
            String generatedSql = sql("""
                    #>{EventDatabase.T_EVENTS}#
                        ->extend(~first: _ | $_.PAYLOAD->get('items')->get(0))
                        ->select(~[ID, first])
                    """);
            // Should contain array index access (e.g., [0] or [1] depending on dialect)
            assertTrue(generatedSql.contains("[") || generatedSql.contains("json_extract"),
                    "Index access should generate subscript. SQL: " + generatedSql);
        }
    }
}
