package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
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
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class JsonM2MIntegrationTest {

    private Connection connection;

    // ==================== Pure Model ====================

    /**
     * Pure model:
     * - RawPerson: JSON source class (no relational mapping)
     * - Person: M2M target with computed fullName
     * - PersonM2MMapping: maps Person ← RawPerson via property expressions
     * - Runtime with JsonModelConnection providing inline JSON data
     */
    private static final String PURE_MODEL = """
            ###Pure
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

            ###Relational
            Database store::TestDb
            (
            )

            ###Connection
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
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
        connection = LaneFloor.backend().open();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Helper ====================

    private String executeGraphFetch(String pureQuery) throws SQLException {
        var result = Lean.run(PURE_MODEL, pureQuery, "test::TestRuntime", connection);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS, result.shape(),
                "graphFetch/serialize answers OBJECTS, got: " + result.shape());
        return result.json();
    }

    private Lean.Result executeProject(String pureQuery) throws SQLException {
        return Lean.run(PURE_MODEL, pureQuery, "test::TestRuntime", connection);
    }

    // ==================== Tests ====================

    @Test
    @DisplayName("project: model::Person.all()->project(~[fullName, age])")
    void testProjectFromJson() throws SQLException {
        var result = executeProject("""
                model::Person.all()->project(~[fullName:x|$x.fullName, age:x|$x.age])
                """);
        assertEquals(3, result.rows().size(), "Should have 3 persons from JSON");

        var colNames = result.columns().stream()
                .map(Lean.Column::name).toList();
        assertTrue(colNames.contains("fullName"), "Should have fullName column");
        assertTrue(colNames.contains("age"), "Should have age column");

        // some row is John Smith's (no sort: not "the first", P3-11)
        assertTrue(result.rows().stream().anyMatch(r -> "John Smith".equals(r.get(colNames.indexOf("fullName")))),
                "a row with fullName John Smith: " + result.rows());
    }

    @Test
    @DisplayName("graphFetch: Person with fullName + age")
    void testGraphFetchFromJson() throws SQLException {
        String json = executeGraphFetch("""
                model::Person.all()
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
    @DisplayName("project with filter: model::Person.all()->filter(x|$x.age > 26)->project(...)")
    void testProjectWithFilter() throws SQLException {
        var result = executeProject("""
                model::Person.all()->filter(x|$x.age > 26)->project(~[fullName:x|$x.fullName])
                """);
        assertEquals(2, result.rows().size(), "Should have 2 persons with age > 26");
    }
}
