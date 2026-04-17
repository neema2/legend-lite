package com.gs.legend.exec;

import com.gs.legend.model.def.MappingDefinition.TestDefinition;
import com.gs.legend.model.def.MappingDefinition.TestSuiteDefinition;
import com.gs.legend.server.QueryService;
import com.gs.legend.util.Json;

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

            // Execute the actual query from the test suite function body
            ExecutionResult result = queryService.execute(
                    fullModel,
                    pureQuery,
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



    /**
     * Serialize a result as a JSON array-of-objects where every value is
     * emitted as a string (preserves the legacy shape so existing test
     * expected-data literals — which quote everything — keep matching).
     * Column names and values are properly RFC 8259 escaped via Json.Writer.
     */
    private String resultToJson(ExecutionResult result) {
        Json.Writer w = Json.compactWriter();
        w.beginArray();
        for (var row : result.rows()) {
            w.beginObject();
            for (int j = 0; j < result.columns().size(); j++) {
                Object v = row.get(j);
                w.field(result.columns().get(j).name(), v == null ? "null" : v.toString());
            }
            w.endObject();
        }
        w.endArray();
        return w.toString();
    }

    /**
     * Deep-compare two JSON documents by parsing both and checking Node
     * equality. Falls back to whitespace-normalized string comparison if
     * either side fails to parse (e.g. historical assertions that aren't
     * strictly valid JSON).
     */
    private boolean compareJson(String actual, String expected) {
        if (expected == null || actual == null) {
            return false;
        }
        try {
            return Json.parse(actual).equals(Json.parse(expected));
        } catch (IllegalArgumentException e) {
            String normalizedActual = actual.replaceAll("\\s+", "");
            String normalizedExpected = expected.replaceAll("\\s+", "");
            return normalizedActual.equals(normalizedExpected);
        }
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
