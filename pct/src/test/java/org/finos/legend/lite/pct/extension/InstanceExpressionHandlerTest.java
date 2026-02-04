package org.finos.legend.lite.pct.extension;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.Result;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for InstanceExpressionHandler.
 * 
 * Tests the handling of PCT queries using InstanceExpression arrays
 * (e.g., [^FirmType(...)...]->project(...)->filter(...))
 */
class InstanceExpressionHandlerTest {

    @Test
    void testRequiresInstanceHandling_withInstanceArray() {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // Simple instance array with project
        String expr = "[^FirmType(legalName = 'Firm X')]->project(~[legalName : x | $x.legalName])";

        // Debug: check what parser produces
        try {
            var ast = org.finos.legend.pure.dsl.PureParser.parse(expr);
            System.out.println("Parsed AST: " + ast);
            System.out.println("AST type: " + ast.getClass().getSimpleName());
            if (ast instanceof org.finos.legend.pure.dsl.MethodCall mc) {
                System.out.println("MethodCall source: " + mc.source());
                System.out.println("MethodCall source type: " + mc.source().getClass().getSimpleName());
            }
        } catch (Exception e) {
            System.out.println("Parse error: " + e);
            e.printStackTrace();
        }

        assertTrue(handler.requiresInstanceHandling(expr),
                "Should detect InstanceExpression array pattern");
    }

    @Test
    void testRequiresInstanceHandling_withClassAll() {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // Standard Class.all() pattern should NOT require instance handling
        String expr = "Person.all()->project(~[firstName : x | $x.firstName])";
        assertFalse(handler.requiresInstanceHandling(expr),
                "Class.all() pattern should not require instance handling");
    }

    @Test
    void testRequiresInstanceHandling_withTdsLiteral() {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // TDS literal pattern should NOT require instance handling
        String expr = "#TDS\nid,name\n1,George\n2,Pierre\n#->filter(x | $x.id == 1)";
        assertFalse(handler.requiresInstanceHandling(expr),
                "TDS literal pattern should not require instance handling");
    }

    @Test
    void testBasicInstanceArrayProject() throws Exception {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // This mirrors the testFilterPostProject PCT pattern (simplified)
        String expr = """
                [
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm X'
                    ),
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm A'
                    )
                ]->project(~[legalName : x | $x.legalName])
                """;

        assertTrue(handler.requiresInstanceHandling(expr),
                "Should detect InstanceExpression pattern");

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Result result = handler.execute(expr, connection);

            assertNotNull(result, "Result should not be null");
            BufferedResult buffered = result.toBuffered();

            // Should have 2 rows (Firm X and Firm A)
            assertEquals(2, buffered.rows().size(), "Should return 2 rows");

            // Check column names
            assertEquals(1, buffered.columns().size(), "Should have 1 column");
            assertEquals("legalName", buffered.columns().get(0).name());

            // Check values
            var rows = buffered.rows();
            assertTrue(
                    rows.stream().anyMatch(r -> "Firm X".equals(r.values().get(0))),
                    "Should contain 'Firm X'");
            assertTrue(
                    rows.stream().anyMatch(r -> "Firm A".equals(r.values().get(0))),
                    "Should contain 'Firm A'");
        }
    }

    @Test
    void testInstanceArrayWithFilter() throws Exception {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // Pattern from testFilterPostProject: project then filter
        String expr = """
                [
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm X'
                    ),
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm A'
                    )
                ]->project(~[legalName : x | $x.legalName])->filter(x | $x.legalName == 'Firm X')
                """;

        assertTrue(handler.requiresInstanceHandling(expr));

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Result result = handler.execute(expr, connection);

            BufferedResult buffered = result.toBuffered();

            // Should have 1 row (only Firm X after filter)
            assertEquals(1, buffered.rows().size(), "Should return 1 row after filter");
            assertEquals("Firm X", buffered.rows().get(0).values().get(0));
        }
    }

    @Test
    void testInstanceArrayWithNestedObjects() throws Exception {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // Full testFilterPostProject pattern with nested employees
        String expr = """
                [
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm X',
                        employees = [
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'Peter', lastName = 'Smith'),
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'John', lastName = 'Johnson')
                        ]
                    ),
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm A',
                        employees = [
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'Fabrice', lastName = 'Roberts')
                        ]
                    )
                ]->project(~[
                    legalName : x | $x.legalName,
                    firstName : x | $x.employees.firstName
                ])->filter(x | $x.legalName == 'Firm X')
                """;

        assertTrue(handler.requiresInstanceHandling(expr));

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Result result = handler.execute(expr, connection);

            BufferedResult buffered = result.toBuffered();

            // Should have rows for Firm X employees (Peter and John)
            System.out.println("Rows: " + buffered.rows().size());
            for (var row : buffered.rows()) {
                System.out.println("  " + row.values());
            }

            // Should have exactly 2 rows for Firm X employees (Peter and John)
            assertEquals(2, buffered.rows().size(), "Should have 2 employees from Firm X");

            // Verify all results are from Firm X
            for (var row : buffered.rows()) {
                assertEquals("Firm X", row.values().get(0), "All rows should be Firm X");
            }
        }
    }

    /**
     * PCT testFilterPostProject pattern with mixed single/array employees.
     * This tests the type normalization fix for DuckDB VALUES clauses.
     * 
     * The actual PCT test has:
     * - Firm X: employees=[...] (4 employees as array)
     * - Firm A: employees=^Person (single employee, NOT in array)
     * 
     * DuckDB requires consistent types in VALUES, so we normalize single values
     * to single-element arrays when other instances have arrays for that property.
     */
    @Test
    void testMixedMultiplicityEmployees() throws Exception {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // Matches actual PCT testFilterPostProject pattern - note Firm A has single
        // employee (no array brackets)
        String expr = """
                [
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm X',
                        employees = [
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'Peter', lastName = 'Smith'),
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'John', lastName = 'Johnson'),
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'John', lastName = 'Hill'),
                            ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'Anthony', lastName = 'Allen')
                        ]
                    ),
                    ^meta::pure::functions::relation::tests::composition::FirmTypeForCompositionTests(
                        legalName = 'Firm A',
                        employees = ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'Fabrice', lastName = 'Roberts')
                    )
                ]->project(~[
                    legalName : x | $x.legalName,
                    firstName : x | $x.employees.firstName
                ])->filter(x | $x.legalName == 'Firm X')
                """;

        assertTrue(handler.requiresInstanceHandling(expr));

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Result result = handler.execute(expr, connection);

            BufferedResult buffered = result.toBuffered();

            // Should have 4 rows for Firm X employees
            assertEquals(4, buffered.rows().size(), "Should have 4 employees from Firm X");

            // Verify all results are from Firm X
            for (var row : buffered.rows()) {
                assertEquals("Firm X", row.values().get(0), "All rows should be Firm X");
            }
        }
    }

    /**
     * PCT testWindowFunctionsAfterProject pattern with aliased projections.
     * Tests that STRUCT field access uses property names, not projection aliases.
     * 
     * Example: first : x | $x.firstName
     * - Output alias: "first"
     * - STRUCT field: "firstName"
     * - Should generate: struct.firstName AS "first"
     */
    @Test
    void testAliasedProjections() throws Exception {
        InstanceExpressionHandler handler = new InstanceExpressionHandler();

        // Projection aliases differ from property names (first vs firstName, last vs
        // lastName)
        String expr = """
                [
                    ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'Peter', lastName = 'Smith'),
                    ^meta::pure::functions::relation::tests::composition::PersonTypeForCompositionTests(firstName = 'John', lastName = 'Johnson')
                ]->project(~[
                    first : x | $x.firstName,
                    last : x | $x.lastName
                ])
                """;

        assertTrue(handler.requiresInstanceHandling(expr));

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Result result = handler.execute(expr, connection);

            BufferedResult buffered = result.toBuffered();

            // Should have 2 rows
            assertEquals(2, buffered.rows().size(), "Should have 2 persons");

            // Column headers should be the aliases (first, last), not the property names
            List<String> columnNames = buffered.columns().stream()
                    .map(col -> col.name())
                    .toList();
            assertEquals(List.of("first", "last"), columnNames, "Column names should be aliases");
        }
    }
}
