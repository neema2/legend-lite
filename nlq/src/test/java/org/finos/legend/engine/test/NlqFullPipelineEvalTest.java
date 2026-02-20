package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.*;
import org.finos.legend.engine.nlq.eval.*;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full pipeline eval that hits the real Gemini API.
 * Skipped unless GEMINI_API_KEY is set.
 *
 * Run with:
 *   GEMINI_API_KEY=... mvn test -pl engine -Dtest="NlqFullPipelineEvalTest"
 */
@EnabledIfEnvironmentVariable(named = "GEMINI_API_KEY", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NlqFullPipelineEvalTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;
    private static GeminiClient gemini;
    private static NlqService service;
    private static NlqEvalRunner runner;
    private static List<NlqEvalCase> evalCases;
    private static List<NlqFullEvalResult> results;

    @BeforeAll
    static void setup() throws Exception {
        InputStream is = NlqFullPipelineEvalTest.class.getResourceAsStream("/nlq/sales-trading-model.pure");
        assertNotNull(is, "Test model not found");
        String pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);

        gemini = new GeminiClient(System.getenv("GEMINI_API_KEY"), "gemini-2.0-flash");
        service = new NlqService(index, modelBuilder, gemini);
        runner = new NlqEvalRunner(index, modelBuilder);

        evalCases = NlqEvalRunner.loadCases("/nlq/nlq-eval-cases.json");

        // Run full pipeline eval for all cases (pipeline + judge = 4 LLM calls per case)
        System.out.println("Running full pipeline eval for " + evalCases.size() + " cases...");
        results = runner.runFullPipelineEval(evalCases, service, gemini);
    }

    // ==================== Aggregate Report ====================

    @Test
    @Order(0)
    @DisplayName("Full pipeline eval report")
    void testFullReport() {
        String report = NlqEvalMetrics.generateFullReport(results);
        System.out.println(report);
    }

    // ==================== Per-Case Dynamic Tests ====================

    @TestFactory
    @Order(1)
    @DisplayName("Per-case pipeline eval")
    Stream<DynamicTest> testPerCase() {
        return results.stream().map(result ->
                DynamicTest.dynamicTest(result.caseId() + ": " + result.question(), () -> {
                    assertFalse(result.hasError(),
                            result.caseId() + " pipeline error: " + result.error());
                    assertNotNull(result.routing(), result.caseId() + " missing routing score");
                    assertNotNull(result.generatedQuery(),
                            result.caseId() + " missing generated query");

                    System.out.printf("  [%s] root=%s(%s) ops=%.0f%% props=%.0f%% judge=%d/5 — %s%n",
                            result.caseId(),
                            result.routing().actual(),
                            result.routing().correct() ? "✓" : "✗",
                            result.queryAccuracy() != null ? result.queryAccuracy().opCoverage() * 100 : 0,
                            result.queryAccuracy() != null ? result.queryAccuracy().propertyCoverage() * 100 : 0,
                            result.llmJudge() != null ? result.llmJudge().score() : 0,
                            result.generatedQuery());
                })
        );
    }

    // ==================== Threshold Assertions ====================

    @Test
    @Order(2)
    @DisplayName("No pipeline errors")
    void testZeroErrors() {
        long errorCount = results.stream().filter(NlqFullEvalResult::hasError).count();
        assertEquals(0, errorCount,
                "Expected zero pipeline errors, got " + errorCount);
    }

    @Test
    @Order(3)
    @DisplayName("Routing accuracy >= 70%")
    void testRoutingAccuracy() {
        long correct = results.stream()
                .filter(r -> !r.hasError() && r.routing() != null && r.routing().correct())
                .count();
        long total = results.stream().filter(r -> !r.hasError()).count();
        double accuracy = (double) correct / Math.max(1, total);

        System.out.printf("Routing accuracy: %d/%d = %.1f%%%n", correct, total, accuracy * 100);
        assertTrue(accuracy >= 0.70,
                String.format("Routing accuracy %.1f%% below 70%% threshold", accuracy * 100));
    }

    @Test
    @Order(4)
    @DisplayName("Average LLM judge score >= 3.0")
    void testJudgeScore() {
        double avgScore = results.stream()
                .filter(r -> !r.hasError() && r.llmJudge() != null && r.llmJudge().score() > 0)
                .mapToInt(r -> r.llmJudge().score())
                .average()
                .orElse(0.0);

        System.out.printf("Average LLM judge score: %.1f / 5.0%n", avgScore);
        assertTrue(avgScore >= 3.0,
                String.format("Average judge score %.1f below 3.0 threshold", avgScore));
    }
}
