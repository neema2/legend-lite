// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The shapes of a query's answer as a client reads lite's JSON rows (the LEAN twin, the result boundary program's
 * track T, 2026-10-29): a scalar or a collection is a VALUE answer of one column {@code value}; a relation is a
 * RELATION answer with the plan's columns; each cell is read by its column's Pure type ({@link Lean}). Until track T
 * this file asserted the old door's result variants ({@code ExecutionResult.Scalar}, {@code Tabular}; the old door's type,
 * deleted in track D) and their
 * {@code returnType}; the same queries are judged on the product's output.
 */
public class ExecutionResultIntegrationTest extends AbstractDatabaseTest {

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        // the backend's own session set-up (the platform's: UTC)
        try (var stmt = connection.createStatement()) {
            for (String sql : backend().sessionSetUp()) {
                stmt.execute(sql);
            }
        }
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Scalar answers ====================

    @Test
    @DisplayName("a scalar integer is a VALUE answer of one Integer cell")
    void scalarInteger() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|42", "test::TestRuntime", connection);

        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, result.shape(), "Expected a VALUE answer for |42");
        assertEquals(42L, result.one());
        assertEquals("Integer", result.columns().get(0).type(), "the plan types the answer's one column");
    }

    @Test
    @DisplayName("a scalar string is a VALUE answer of one String cell")
    void scalarString() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|'hello'", "test::TestRuntime", connection);

        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, result.shape());
        assertEquals("hello", result.one());
    }

    @Test
    @DisplayName("a scalar boolean is a VALUE answer of one Boolean cell")
    void scalarBoolean() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|true", "test::TestRuntime", connection);

        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, result.shape());
        assertEquals(true, result.one());
    }

    @Test
    @DisplayName("a scalar expression (1+2) is a VALUE answer")
    void scalarExpression() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|1 + 2", "test::TestRuntime", connection);

        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, result.shape());
        assertEquals(3L, result.one());
    }

    // ==================== Relation answers ====================

    @Test
    @DisplayName("model::Person.all()->project is a RELATION answer with the plan's columns")
    void tabularGetAllProject() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|model::Person.all()->project(~[firstName, lastName])->from(model::PersonMapping, test::TestRuntime)", "test::TestRuntime", connection);

        assertEquals(ExecutionPlan.TdsResult.Shape.RELATION, result.shape(), "Expected a RELATION answer for a project query");
        assertTrue(result.rowCount() > 0, "Should have rows");
        assertEquals(2, result.columnCount(), "Should have 2 columns (firstName, lastName)");
        assertEquals("firstName", result.columns().get(0).name());
        assertEquals("String", result.columns().get(0).type());
    }

    @Test
    @DisplayName("model::Person.all()->filter->project is a RELATION answer")
    void tabularFilterProject() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|model::Person.all()->filter(p|$p.age > 25)->project(~[firstName, age])->from(model::PersonMapping, test::TestRuntime)", "test::TestRuntime", connection);

        assertEquals(ExecutionPlan.TdsResult.Shape.RELATION, result.shape());
        assertTrue(result.rowCount() >= 2, "John(30) and Bob(45) should pass age>25 filter");
    }

    // ==================== A VALUE answer's one column ====================

    @Test
    @DisplayName("a VALUE answer is written as rows of one column named value")
    void valueAnswerHasOneColumnNamedValue() throws SQLException {
        Lean.Result result = Lean.run(getCompletePureModelWithRuntime(), "|42", "test::TestRuntime", connection);

        assertEquals(1, result.columnCount());
        assertEquals("value", result.columns().get(0).name(), "lite's JSON rows key a value by 'value'");
        assertTrue(result.text().startsWith("[{\"value\":"), "the JSON as written: " + result.text());
    }

    // ==================== Every column is typed ====================

    @Test
    @DisplayName("the plan types every answer column")
    void returnTypeNeverNull() throws SQLException {
        // a scalar
        Lean.Result scalar = Lean.run(getCompletePureModelWithRuntime(), "|99", "test::TestRuntime", connection);
        assertEquals("Integer", scalar.columns().get(0).type(), "a scalar's column is typed");

        // a relation
        Lean.Result tabular = Lean.run(getCompletePureModelWithRuntime(), "|model::Person.all()->project(~[firstName])->from(model::PersonMapping, test::TestRuntime)", "test::TestRuntime", connection);
        assertEquals("String", tabular.columns().get(0).type(), "a relation's column is typed");
    }
}
