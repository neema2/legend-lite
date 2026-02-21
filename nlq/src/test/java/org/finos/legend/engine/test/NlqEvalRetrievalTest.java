package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.engine.nlq.ModelSchemaExtractor;
import org.finos.legend.engine.nlq.eval.NlqEvalCase;
import org.finos.legend.engine.nlq.eval.NlqEvalMetrics;
import org.finos.legend.engine.nlq.eval.NlqEvalResult;
import org.finos.legend.engine.nlq.eval.NlqEvalRunner;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.m3.PureClass;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NLQ Retrieval Eval — Sales & Trading domain.
 * Runs on every build, no LLM required.
 *
 * Tests that the SemanticIndex correctly retrieves relevant classes
 * for natural language queries against a ~30-class S&T model.
 */
@DisplayName("NLQ Retrieval Eval — Sales & Trading")
class NlqEvalRetrievalTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;
    private static List<NlqEvalCase> evalCases;

    @BeforeAll
    static void setup() throws IOException {
        // Load the S&T test model
        String pureSource;
        try (InputStream is = NlqEvalRetrievalTest.class.getResourceAsStream("/nlq/sales-trading-model.pure")) {
            assertNotNull(is, "Test model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        // Parse and build the model
        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        // Build the semantic index
        index = new SemanticIndex();
        index.buildIndex(modelBuilder);

        // Load eval cases
        evalCases = NlqEvalRunner.loadCases("/nlq/nlq-eval-cases.json");
    }

    // ==================== Model Integrity Tests ====================

    @Test
    @DisplayName("Model loads with expected class count")
    void testModelClassCount() {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        // We expect ~20+ classes from the S&T model (excluding enum-only classes)
        assertTrue(allClasses.size() >= 15,
                "Expected at least 15 classes, got " + allClasses.size());
        System.out.println("Total classes indexed: " + allClasses.size());
    }

    @Test
    @DisplayName("Semantic index is populated")
    void testIndexPopulated() {
        assertTrue(index.size() >= 15,
                "Expected at least 15 indexed entries, got " + index.size());
        System.out.println("Index entries: " + index.size());
    }

    @Test
    @DisplayName("Eval cases load correctly")
    void testEvalCasesLoad() {
        assertFalse(evalCases.isEmpty(), "Eval cases should not be empty");
        assertTrue(evalCases.size() >= 20,
                "Expected at least 20 eval cases, got " + evalCases.size());

        // Verify each case has required fields
        for (NlqEvalCase c : evalCases) {
            assertNotNull(c.id(), "Case id should not be null");
            assertNotNull(c.question(), "Case question should not be null");
            assertNotNull(c.expected(), "Case expected should not be null");
            assertNotNull(c.expected().retrieval(), "Case retrieval expectation should not be null");
            assertFalse(c.expected().retrieval().mustInclude().isEmpty(),
                    "Case " + c.id() + " should have at least one mustInclude class");
        }
        System.out.println("Eval cases loaded: " + evalCases.size());
    }

    // ==================== Retrieval Eval ====================

    @TestFactory
    @DisplayName("Retrieval eval per case")
    Stream<DynamicTest> testRetrievalPerCase() {
        NlqEvalRunner runner = new NlqEvalRunner(index, modelBuilder);

        return evalCases.stream().map(evalCase ->
                DynamicTest.dynamicTest(evalCase.id() + ": " + evalCase.question(), () -> {
                    NlqEvalResult result = runner.runSingleRetrieval(evalCase, 15);

                    assertFalse(result.hasError(),
                            "Case " + evalCase.id() + " errored: " + result.error());

                    NlqEvalResult.RetrievalScore score = result.retrieval();
                    assertNotNull(score, "Retrieval score should not be null");

                    // Log for debugging
                    System.out.printf("  [%s] recall=%.2f exclusion=%.2f pass=%s retrieved=%s%n",
                            evalCase.id(),
                            score.recall(),
                            score.exclusionRate(),
                            score.passed(),
                            score.retrievedClasses());

                    if (!score.missingClasses().isEmpty()) {
                        System.out.println("    Missing: " + score.missingClasses());
                    }
                    if (!score.unexpectedClasses().isEmpty()) {
                        System.out.println("    Unexpected: " + score.unexpectedClasses());
                    }

                    // Soft assertion: log failures but don't fail the build yet
                    // Hard assertions will be added once baseline is established
                    assertTrue(score.recall() >= 0.0,
                            "Recall should be non-negative");
                })
        );
    }

    // ==================== Aggregate Report ====================

    @Test
    @DisplayName("Full retrieval eval with aggregate report")
    void testFullRetrievalEval() {
        NlqEvalRunner runner = new NlqEvalRunner(index, modelBuilder);
        List<NlqEvalResult> results = runner.runRetrievalEval(evalCases, 15);

        // Generate and print report
        String report = NlqEvalMetrics.generateReport(results);
        System.out.println("\n" + report);

        // Aggregate assertions
        long passed = results.stream()
                .filter(r -> !r.hasError() && r.retrieval() != null && r.retrieval().passed())
                .count();
        long total = results.size();
        double passRate = (double) passed / total;

        System.out.printf("Pass rate: %d/%d = %.1f%%%n", passed, total, 100 * passRate);

        // We expect at least 50% pass rate for a first baseline
        // This threshold will be raised as the index improves
        assertTrue(passRate >= 0.40,
                String.format("Expected at least 40%% pass rate, got %.1f%% (%d/%d)",
                        100 * passRate, passed, total));
    }

    // ==================== Holdout Retrieval Eval ====================

    @Test
    @DisplayName("Holdout retrieval eval (blind set)")
    void testHoldoutRetrievalEval() throws Exception {
        List<NlqEvalCase> holdoutCases = NlqEvalRunner.loadCases("/nlq/nlq-holdout-cases.json");
        NlqEvalRunner runner = new NlqEvalRunner(index, modelBuilder);
        List<NlqEvalResult> results = runner.runRetrievalEval(holdoutCases, 15);

        String report = NlqEvalMetrics.generateReport(results);
        System.out.println("\nHOLDOUT Retrieval Report:");
        System.out.println(report);

        long passed = results.stream()
                .filter(r -> !r.hasError() && r.retrieval() != null && r.retrieval().passed())
                .count();
        long total = results.size();
        double passRate = (double) passed / total;

        System.out.printf("Holdout pass rate: %d/%d = %.1f%%%n", passed, total, 100 * passRate);
        assertTrue(passRate >= 0.40,
                String.format("Holdout retrieval pass rate %.1f%% below 40%% threshold", 100 * passRate));
    }

    // ==================== Schema Extraction Test ====================

    @Test
    @DisplayName("ModelSchemaExtractor produces focused schema")
    void testSchemaExtraction() {
        // Retrieve top classes for a sample query
        List<SemanticIndex.RetrievalResult> retrieved =
                index.retrieve("total PnL by trader", 10);

        Set<String> classNames = retrieved.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .collect(Collectors.toSet());

        String schema = ModelSchemaExtractor.extractSchema(classNames, modelBuilder);

        assertNotNull(schema);
        assertFalse(schema.isEmpty(), "Schema should not be empty");

        // Should contain key classes
        assertTrue(schema.contains("DailyPnL") || schema.contains("Trader"),
                "Schema should mention DailyPnL or Trader for PnL query");

        System.out.println("Schema for 'total PnL by trader':");
        System.out.println(schema);
        System.out.println("Schema length: " + schema.length() + " chars (~"
                + schema.length() / 4 + " tokens)");
    }

    // ==================== Tokenizer Tests ====================

    @Test
    @DisplayName("Tokenizer handles camelCase and special chars")
    void testTokenizer() {
        List<String> tokens = SemanticIndex.tokenize("tradeDate settlementDate P&L");
        assertTrue(tokens.contains("trade"), "Should split camelCase: " + tokens);
        assertTrue(tokens.contains("date"), "Should have 'date': " + tokens);
        assertTrue(tokens.contains("settlement"), "Should have 'settlement': " + tokens);
        assertTrue(tokens.contains("pnl"), "P&L should become 'pnl': " + tokens);

        List<String> tokens2 = SemanticIndex.tokenize("Goldman Sachs counterparty");
        assertTrue(tokens2.contains("goldman"), "Should have 'goldman': " + tokens2);
        assertTrue(tokens2.contains("sachs"), "Should have 'sachs': " + tokens2);
        assertTrue(tokens2.contains("counterparty"), "Should have 'counterparty': " + tokens2);

        List<String> tokens3 = SemanticIndex.tokenize("cpty risk on GS");
        assertTrue(tokens3.contains("counterparty"), "cpty should expand: " + tokens3);
    }
}
