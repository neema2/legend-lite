package org.finos.legend.lite.pct;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple PCT-style tests for Legend-Lite relation functions.
 * 
 * These tests validate that legend-lite correctly implements Pure relation
 * functions
 * using inline TDS literals as test data - the same pattern used by
 * legend-engine's PCT.
 * 
 * Pattern:
 * 1. Define test data using #TDS literal
 * 2. Apply relation function (filter, extend, sort, etc.)
 * 3. Validate result matches expected output
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("PCT-Style Relation Function Tests")
public class SimplePCTTests {

    private Connection connection;
    private QueryService queryService;

    // Minimal Pure model with DuckDB connection for TDS literal execution
    private static final String PURE_MODEL = """
            Class test::Dummy { value: Integer[1]; }

            Database store::TestDatabase (
                Table DUMMY (VALUE INTEGER)
            )

            Mapping test::DummyMapping (
                Dummy: Relational {
                    ~mainTable [TestDatabase] DUMMY
                    value: [TestDatabase] DUMMY.VALUE
                }
            )

            RelationalDatabaseConnection store::TestConnection {
                type: DuckDB;
                specification: InMemory { };
            }

            Runtime test::TestRuntime {
                mappings: [test::DummyMapping];
                connections: [
                    store::TestDatabase: [environment: store::TestConnection]
                ];
            }
            """;

    @BeforeAll
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        queryService = new QueryService();
    }

    @AfterAll
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * Execute a TDS literal expression and return the result.
     */
    private BufferedResult executeTds(String tdsExpression) throws SQLException {
        return queryService.execute(PURE_MODEL, tdsExpression, "test::TestRuntime", connection);
    }

    // ==================== filter() Tests ====================

    @Test
    @DisplayName("filter() - basic numeric comparison")
    void testFilterNumericComparison() throws Exception {
        String pureQuery = """
                #TDS
                name, age
                Alice, 25
                Bob, 30
                Charlie, 35
                #
                ->filter(r | $r.age > 28)
                """;

        BufferedResult result = executeTds(pureQuery);

        // Should filter to Bob (30) and Charlie (35)
        assertEquals(2, result.rows().size(), "Filter should return 2 rows");

        // Validate column structure
        assertEquals(2, result.columns().size());
        assertEquals("name", result.columns().get(0).name());
        assertEquals("age", result.columns().get(1).name());
    }

    @Test
    @DisplayName("filter() - string equality")
    void testFilterStringEquality() throws Exception {
        String pureQuery = """
                #TDS
                name, dept
                Alice, Engineering
                Bob, Sales
                Charlie, Engineering
                #
                ->filter(r | $r.dept == 'Engineering')
                """;

        BufferedResult result = executeTds(pureQuery);

        assertEquals(2, result.rows().size(), "Filter should return Alice and Charlie");
    }

    // ==================== sort() Tests ====================

    @Test
    @DisplayName("sort() - ascending order")
    void testSortAscending() throws Exception {
        String pureQuery = """
                #TDS
                name, score
                Charlie, 75
                Alice, 90
                Bob, 85
                #
                ->sort(~score)
                """;

        BufferedResult result = executeTds(pureQuery);

        assertEquals(3, result.rows().size());
        // First row should be Charlie (75)
        assertEquals("Charlie", result.rows().get(0).values().get(0));
    }

    // ==================== limit() Tests ====================

    @Test
    @DisplayName("limit() - take first N rows")
    void testLimit() throws Exception {
        String pureQuery = """
                #TDS
                id, value
                1, A
                2, B
                3, C
                4, D
                #
                ->limit(2)
                """;

        BufferedResult result = executeTds(pureQuery);

        assertEquals(2, result.rows().size(), "Limit should return exactly 2 rows");
    }

    // ==================== Basic TDS Execution ====================

    @Test
    @DisplayName("TDS literal - basic execution without any operations")
    void testBasicTdsExecution() throws Exception {
        String pureQuery = """
                #TDS
                a, b
                1, hello
                2, world
                #
                """;

        BufferedResult result = executeTds(pureQuery);

        assertEquals(2, result.columns().size());
        assertEquals(2, result.rows().size());
        assertEquals("a", result.columns().get(0).name());
        assertEquals("b", result.columns().get(1).name());
    }
}
