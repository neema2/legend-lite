package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.ExecutionResult.ScalarResult;
import com.gs.legend.exec.ExecutionResult.TabularResult;
import com.gs.legend.plan.GenericType;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that exercise {@code QueryService.execute()}.
 * Every test calls the NEW typed path end-to-end:
 *   Pure query → TypeChecker (stamps returnType) → PlanGenerator → PlanExecutor.executeTyped → ExecutionResult
 *
 * Validates both the data AND the result variant/type metadata.
 */
public class ExecutionResultIntegrationTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() { return "DuckDB"; }

    @Override
    protected SQLDialect getDialect() { return DuckDBDialect.INSTANCE; }

    @Override
    protected String getJdbcUrl() { return "jdbc:duckdb:"; }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        try (var stmt = connection.createStatement()) {
            stmt.execute("SET timezone='UTC'");
        }
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Scalar Results ====================

    @Test
    @DisplayName("executeTyped: scalar integer returns ScalarResult")
    void scalarInteger() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|42",
                "test::TestRuntime",
                connection);

        assertInstanceOf(ScalarResult.class, result, "Expected ScalarResult for |42");
        ScalarResult scalar = result.asScalar();
        assertEquals(42L, ((Number) scalar.value()).longValue());
        assertNotNull(scalar.returnType(), "returnType must be non-null");
    }

    @Test
    @DisplayName("executeTyped: scalar string returns ScalarResult")
    void scalarString() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'hello'",
                "test::TestRuntime",
                connection);

        assertInstanceOf(ScalarResult.class, result);
        assertEquals("hello", result.asScalar().value());
    }

    @Test
    @DisplayName("executeTyped: scalar boolean returns ScalarResult")
    void scalarBoolean() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|true",
                "test::TestRuntime",
                connection);

        assertInstanceOf(ScalarResult.class, result);
        assertEquals(true, result.asScalar().value());
    }

    @Test
    @DisplayName("executeTyped: scalar expression (1+2) returns ScalarResult")
    void scalarExpression() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1 + 2",
                "test::TestRuntime",
                connection);

        assertInstanceOf(ScalarResult.class, result);
        assertEquals(3L, ((Number) result.asScalar().value()).longValue());
    }

    // ==================== Tabular Results ====================

    @Test
    @DisplayName("executeTyped: Person.all()->project returns TabularResult")
    void tabularGetAllProject() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|model::Person.all()->project(~[firstName, lastName])->from(model::PersonMapping, test::TestRuntime)",
                "test::TestRuntime",
                connection);

        assertInstanceOf(TabularResult.class, result, "Expected TabularResult for project query");
        TabularResult tabular = result.asTabular();
        assertNotNull(tabular.returnType());
        assertInstanceOf(GenericType.Relation.class, tabular.returnType(),
                "returnType should be Relation for tabular result");
        assertTrue(tabular.rowCount() > 0, "Should have rows");
        assertEquals(2, tabular.columnCount(), "Should have 2 columns (firstName, lastName)");
    }

    @Test
    @DisplayName("executeTyped: Person.all()->filter->project returns TabularResult")
    void tabularFilterProject() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|model::Person.all()->filter(p|$p.age > 25)->project(~[firstName, age])->from(model::PersonMapping, test::TestRuntime)",
                "test::TestRuntime",
                connection);

        assertInstanceOf(TabularResult.class, result);
        TabularResult tabular = result.asTabular();
        assertTrue(tabular.rowCount() >= 2, "John(30) and Bob(45) should pass age>25 filter");
    }

    // ==================== Typed accessor error handling ====================

    @Test
    @DisplayName("executeTyped: asTabular() on ScalarResult throws with clear message")
    void typedAccessorErrorMessage() throws SQLException {
        ExecutionResult result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|42",
                "test::TestRuntime",
                connection);

        var ex = assertThrows(IllegalStateException.class, result::asTabular);
        assertTrue(ex.getMessage().contains("ScalarResult"),
                "Error should mention actual type: " + ex.getMessage());
    }

    // ==================== returnType is never null ====================

    @Test
    @DisplayName("executeTyped: returnType is always non-null")
    void returnTypeNeverNull() throws SQLException {
        // Scalar
        ExecutionResult scalar = queryService.execute(
                getCompletePureModelWithRuntime(), "|99", "test::TestRuntime", connection);
        assertNotNull(scalar.returnType(), "Scalar returnType must not be null");

        // Tabular
        ExecutionResult tabular = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|model::Person.all()->project(~firstName)->from(model::PersonMapping, test::TestRuntime)",
                "test::TestRuntime", connection);
        assertNotNull(tabular.returnType(), "Tabular returnType must not be null");
    }
}
