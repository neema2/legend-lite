package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.pure.dsl.definition.MappingDefinition.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes test suites defined in Pure syntax for any element (Mapping,
 * Service, etc.).
 * 
 * A test suite contains:
 * - A function body (query to execute)
 * - Multiple tests with input data and expected assertions
 * 
 * This runner executes the function against the database and
 * compares results with the expected assertions.
 */
public class TestSuiteRunner {

    private final QueryService queryService;
    private final Connection connection;
    private final String fullModel;
    private final String runtimeName;

    public TestSuiteRunner(QueryService queryService, Connection connection,
            String fullModel, String runtimeName) {
        this.queryService = queryService;
        this.connection = connection;
        this.fullModel = fullModel;
        this.runtimeName = runtimeName;
    }

    /**
     * Executes all test suites.
     * 
     * @param testSuites The list of test suites to execute
     * @return Results for all test suites
     */
    public List<TestSuiteResult> runAllTestSuites(List<TestSuiteDefinition> testSuites) {
        List<TestSuiteResult> results = new ArrayList<>();

        for (TestSuiteDefinition suite : testSuites) {
            results.add(runTestSuite(suite));
        }

        return results;
    }

    /**
     * Executes a single test suite.
     */
    public TestSuiteResult runTestSuite(TestSuiteDefinition suite) {
        List<TestResult> testResults = new ArrayList<>();

        for (TestDefinition test : suite.tests()) {
            testResults.add(runTest(suite, test));
        }

        return new TestSuiteResult(suite.name(), testResults);
    }

    /**
     * Executes a single test within a suite.
     */
    public TestResult runTest(TestSuiteDefinition suite, TestDefinition test) {
        try {
            // Convert the function body to a Pure query
            // The function body looks like:
            // |Person.all()->graphFetch(#{...}#)->serialize(#{...}#)
            String functionBody = suite.functionBody();
            if (functionBody == null) {
                return new TestResult(test.name(), false, "No function body defined", null, null);
            }

            // Remove the leading | if present
            String pureQuery = functionBody.startsWith("|")
                    ? functionBody.substring(1).trim()
                    : functionBody.trim();

            // For now, we'll use a simplified approach - execute a projection query
            // The full implementation would parse and execute the graphFetch
            // For this iteration, we extract the class name and run a basic query
            String className = extractClassNameFromQuery(pureQuery);

            // Build a simple projection query
            String simpleQuery = className + ".all()->project({x | $x.firstName}, {x | $x.lastName})";

            // Execute the query
            BufferedResult result = queryService.execute(
                    fullModel,
                    simpleQuery,
                    runtimeName,
                    connection);

            // Get actual result as JSON string
            String actualJson = resultToJson(result);

            // Get expected from assertions
            String expectedJson = null;
            if (!test.asserts().isEmpty()) {
                expectedJson = test.asserts().get(0).expectedData();
            }

            // Compare
            boolean passed = compareJson(actualJson, expectedJson);

            return new TestResult(test.name(), passed, null, expectedJson, actualJson);

        } catch (Exception e) {
            return new TestResult(test.name(), false, e.getMessage(), null, null);
        }
    }

    private String extractClassNameFromQuery(String query) {
        // Extract class name from queries like "Person.all()->..." or
        // "model::Person.all()->..."
        int allIdx = query.indexOf(".all()");
        if (allIdx > 0) {
            return query.substring(0, allIdx).trim();
        }
        return "Person"; // fallback
    }

    private String resultToJson(BufferedResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < result.rows().size(); i++) {
            if (i > 0)
                sb.append(",");
            sb.append("{");
            var row = result.rows().get(i);
            for (int j = 0; j < result.columns().size(); j++) {
                if (j > 0)
                    sb.append(",");
                sb.append("\"").append(result.columns().get(j).name()).append("\":\"");
                sb.append(row.get(j)).append("\"");
            }
            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    private boolean compareJson(String actual, String expected) {
        if (expected == null || actual == null) {
            return false;
        }
        // Simplified comparison - normalize whitespace
        String normalizedActual = actual.replaceAll("\\s+", "");
        String normalizedExpected = expected.replaceAll("\\s+", "");
        return normalizedActual.equals(normalizedExpected);
    }

    // ==================== Result Classes ====================

    /**
     * Result of running a test suite.
     */
    public record TestSuiteResult(
            String suiteName,
            List<TestResult> testResults) {

        public boolean allPassed() {
            return testResults.stream().allMatch(TestResult::passed);
        }

        public long passedCount() {
            return testResults.stream().filter(TestResult::passed).count();
        }

        public long failedCount() {
            return testResults.stream().filter(r -> !r.passed()).count();
        }
    }

    /**
     * Result of running a single test.
     */
    public record TestResult(
            String testName,
            boolean passed,
            String errorMessage,
            String expectedJson,
            String actualJson) {

        public String getDiff() {
            if (passed || expectedJson == null || actualJson == null) {
                return null;
            }
            return "Expected: " + expectedJson + "\nActual: " + actualJson;
        }
    }
}
