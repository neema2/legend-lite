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
 * Holdout (blind) eval set — measures generalization.
 * These cases should NEVER be used to tune prompts, metadata, or examples.
 * Skipped unless GEMINI_API_KEY is set.
 *
 * Run with:
 *   GEMINI_API_KEY=... mvn test -pl nlq -Dtest="NlqHoldoutEvalTest"
 */
@EnabledIfEnvironmentVariable(named = "GEMINI_API_KEY", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NlqHoldoutEvalTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;
    private static GeminiClient gemini;
    private static NlqService service;
    private static NlqEvalRunner runner;
    private static List<NlqEvalCase> evalCases;
    private static List<NlqFullEvalResult> results;

    @BeforeAll
    static void setup() throws Exception {
        InputStream is = NlqHoldoutEvalTest.class.getResourceAsStream("/nlq/sales-trading-model.pure");
        assertNotNull(is, "Test model not found");
        String pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);

        gemini = new GeminiClient(System.getenv("GEMINI_API_KEY"), "gemini-2.0-flash");
        service = new NlqService(index, modelBuilder, gemini);
        runner = new NlqEvalRunner(index, modelBuilder);

        evalCases = NlqEvalRunner.loadCases("/nlq/nlq-holdout-cases.json");

        System.out.println("Running HOLDOUT eval for " + evalCases.size() + " cases...");
        results = runner.runFullPipelineEval(evalCases, service, gemini);
    }

    // ==================== Aggregate Report ====================

    @Test
    @Order(0)
    @DisplayName("Holdout eval report")
    void testHoldoutReport() {
        String report = NlqEvalMetrics.generateFullReport(results);
        System.out.println(report);
    }

    // ==================== Per-Case Dynamic Tests ====================

    @TestFactory
    @Order(1)
    @DisplayName("Per-case holdout eval")
    Stream<DynamicTest> testPerCase() {
        return results.stream().map(result ->
                DynamicTest.dynamicTest(result.caseId() + ": " + result.question(), () -> {
                    assertFalse(result.hasError(),
                            result.caseId() + " pipeline error: " + result.error());
                    assertNotNull(result.routing(), result.caseId() + " missing routing score");
                    assertNotNull(result.generatedQuery(),
                            result.caseId() + " missing generated query");

                    String routeIcon = result.routing().preferredMatch() ? "✓"
                            : result.routing().acceptableMatch() ? "~" : "✗";
                    System.out.printf("  [%s] root=%s(%s) ops=%.0f%% roles=%.0f%% judge=%d/5 — %s%n",
                            result.caseId(),
                            result.routing().actual(),
                            routeIcon,
                            result.queryAccuracy() != null ? result.queryAccuracy().opCoverage() * 100 : 0,
                            result.propertyRoles() != null ? result.propertyRoles().overallScore() * 100 : 0,
                            result.llmJudge() != null ? result.llmJudge().overall() : 0,
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
    @DisplayName("Acceptable routing >= 70%")
    void testRoutingAccuracy() {
        long acceptable = results.stream()
                .filter(r -> !r.hasError() && r.routing() != null && r.routing().acceptableMatch())
                .count();
        long total = results.stream().filter(r -> !r.hasError()).count();
        double accuracy = (double) acceptable / Math.max(1, total);

        long preferred = results.stream()
                .filter(r -> !r.hasError() && r.routing() != null && r.routing().preferredMatch())
                .count();

        System.out.printf("HOLDOUT Routing: preferred=%d/%d (%.1f%%), acceptable=%d/%d (%.1f%%)%n",
                preferred, total, 100.0 * preferred / Math.max(1, total),
                acceptable, total, accuracy * 100);
        assertTrue(accuracy >= 0.70,
                String.format("Holdout acceptable routing %.1f%% below 70%% threshold", accuracy * 100));
    }

    @Test
    @Order(4)
    @DisplayName("Average LLM judge overall >= 3.0")
    void testJudgeScore() {
        double avgScore = results.stream()
                .filter(r -> !r.hasError() && r.llmJudge() != null && r.llmJudge().overall() > 0)
                .mapToInt(r -> r.llmJudge().overall())
                .average()
                .orElse(0.0);

        System.out.printf("HOLDOUT Average LLM judge overall: %.1f / 5.0%n", avgScore);
        assertTrue(avgScore >= 3.0,
                String.format("Holdout average judge overall %.1f below 3.0 threshold", avgScore));
    }
}
