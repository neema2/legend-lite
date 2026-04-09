package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JSON-source M2M (Model-to-Model) transforms.
 *
 * These tests verify the FULL JSON→M2M execution path:
 *   JsonModelConnection → variantIdentity mapping → TypeChecker → PlanGenerator
 *   → inline JSON subquery in FROM → PlanExecutor (single SELECT) → result
 *
 * Unlike standard M2M tests (which have a relational mapping for the source class),
 * these tests have NO database tables at all — data comes from inline JSON via
 * {@code data:application/json,...} URIs in the runtime's JsonModelConnection.
 */
@DisplayName("JSON Source M2M Integration Tests")
class JsonM2MIntegrationTest {

    private Connection connection;
    private final QueryService queryService = new QueryService();

    // ==================== Pure Model ====================

    /**
     * Pure model:
     * - RawPerson: JSON source class (no relational mapping)
     * - Person: M2M target with computed fullName
     * - PersonM2MMapping: maps Person ← RawPerson via property expressions
     * - Runtime with JsonModelConnection providing inline JSON data
     */
    private static final String PURE_MODEL = """
            import model::*;
            import store::*;

            Class model::RawPerson
            {
                firstName: String[1];
                lastName:  String[1];
                age:       Integer[1];
            }

            Class model::Person
            {
                fullName: String[1];
                age:      Integer[1];
            }

            ###Mapping
            Mapping model::PersonM2MMapping
            (
                Person: Pure
                {
                    ~src RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName,
                    age: $src.age
                }
            )

            ###Connection
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            ###Runtime
            Runtime test::TestRuntime
            {
                mappings:
                [
                    model::PersonM2MMapping
                ];
                connections:
                [
                    store::TestDb:
                    [
                        env: store::TestConnection
                    ],
                    ModelStore:
                    [
                        json: #{
                            JsonModelConnection
                            {
                                class: model::RawPerson;
                                url: 'data:application/json,[{"firstName":"John","lastName":"Smith","age":30},{"firstName":"Jane","lastName":"Doe","age":25},{"firstName":"Bob","lastName":"Jones","age":45}]';
                            }
                        }#
                    ]
                ];
            }
            """;

    // ==================== Setup / Teardown ====================

    @BeforeEach
    void setUp() throws SQLException {
        // In-memory DuckDB — no test data to set up!
        // All data comes from JsonModelConnection.
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Helper ====================

    private String executeGraphFetch(String pureQuery) throws SQLException {
        var result = queryService.execute(PURE_MODEL, pureQuery, "test::TestRuntime", connection);
        assertInstanceOf(ExecutionResult.GraphResult.class, result,
                "graphFetch/serialize should return GraphResult, got: " + result.getClass().getSimpleName());
        return result.asGraph().json();
    }

    private ExecutionResult executeProject(String pureQuery) throws SQLException {
        return queryService.execute(PURE_MODEL, pureQuery, "test::TestRuntime", connection);
    }

    // ==================== Tests ====================

    @Test
    @DisplayName("project: Person.all()->project(~[fullName, age])")
    void testProjectFromJson() throws SQLException {
        var result = executeProject("""
                Person.all()->project(~[fullName:x|$x.fullName, age:x|$x.age])
                """);
        var tabular = result.asTabular();
        assertEquals(3, tabular.rows().size(), "Should have 3 persons from JSON");

        var colNames = tabular.columns().stream()
                .map(com.gs.legend.exec.Column::name).toList();
        assertTrue(colNames.contains("fullName"), "Should have fullName column");
        assertTrue(colNames.contains("age"), "Should have age column");

        // Check first row
        var firstRow = tabular.rows().get(0);
        assertEquals("John Smith", firstRow.get(colNames.indexOf("fullName")));
    }

    @Test
    @DisplayName("graphFetch: Person with fullName + age")
    void testGraphFetchFromJson() throws SQLException {
        String json = executeGraphFetch("""
                Person.all()
                    ->graphFetch(#{ Person { fullName, age } }#)
                    ->serialize(#{ Person { fullName, age } }#)
                """);
        assertTrue(json.contains("John Smith"), "Should contain 'John Smith', got: " + json);
        assertTrue(json.contains("Jane Doe"), "Should contain 'Jane Doe', got: " + json);
        assertTrue(json.contains("Bob Jones"), "Should contain 'Bob Jones', got: " + json);
        // Age should be present as number
        assertTrue(json.contains("30"), "Should contain age 30, got: " + json);
    }

    @Test
    @DisplayName("project with filter: Person.all()->filter(x|$x.age > 26)->project(...)")
    void testProjectWithFilter() throws SQLException {
        var result = executeProject("""
                Person.all()->filter(x|$x.age > 26)->project(~[fullName:x|$x.fullName])
                """);
        var tabular = result.asTabular();
        assertEquals(2, tabular.rows().size(), "Should have 2 persons with age > 26");
    }
}
