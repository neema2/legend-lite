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
 * CDM model eval — tests NLQ pipeline against the ISDA Common Domain Model (741 classes).
 * Skipped unless GEMINI_API_KEY is set.
 *
 * Run with:
 *   GEMINI_API_KEY=... mvn test -pl nlq -Dtest="NlqCdmEvalTest"
 */
@EnabledIfEnvironmentVariable(named = "GEMINI_API_KEY", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NlqCdmEvalTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;
    private static GeminiClient gemini;
    private static NlqService service;
    private static NlqEvalRunner runner;
    private static List<NlqEvalCase> evalCases;
    private static List<NlqFullEvalResult> results;

    @BeforeAll
    static void setup() throws Exception {
        InputStream is = NlqCdmEvalTest.class.getResourceAsStream("/nlq/cdm-model.pure");
        assertNotNull(is, "CDM model not found");
        String pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);

        gemini = new GeminiClient(System.getenv("GEMINI_API_KEY"), "gemini-3-flash-preview");
        service = new NlqService(index, modelBuilder, gemini);
        runner = new NlqEvalRunner(index, modelBuilder);

        evalCases = NlqEvalRunner.loadCases("/nlq/cdm-eval-cases.json");

        System.out.println("Running CDM eval for " + evalCases.size() + " cases (" + 
                modelBuilder.getAllClasses().size() + " classes)...");
        results = runner.runFullPipelineEval(evalCases, service, gemini);
    }

    // ==================== Aggregate Report ====================

    @Test
    @Order(0)
    @DisplayName("CDM full pipeline eval report")
    void testFullReport() {
        String report = NlqEvalMetrics.generateFullReport(results);
        System.out.println(report);
    }

    // ==================== Per-Case Dynamic Tests ====================

    @TestFactory
    @Order(1)
    @DisplayName("CDM per-case pipeline eval")
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
    @DisplayName("Acceptable routing >= 50% (CDM has 741 classes)")
    void testRoutingAccuracy() {
        long acceptable = results.stream()
                .filter(r -> !r.hasError() && r.routing() != null && r.routing().acceptableMatch())
                .count();
        long total = results.stream().filter(r -> !r.hasError()).count();
        double accuracy = (double) acceptable / Math.max(1, total);

        long preferred = results.stream()
                .filter(r -> !r.hasError() && r.routing() != null && r.routing().preferredMatch())
                .count();

        System.out.printf("Routing: preferred=%d/%d (%.1f%%), acceptable=%d/%d (%.1f%%)%n",
                preferred, total, 100.0 * preferred / Math.max(1, total),
                acceptable, total, accuracy * 100);
        assertTrue(accuracy >= 0.50,
                String.format("Acceptable routing %.1f%% below 50%% threshold", accuracy * 100));
    }

    @Test
    @Order(4)
    @DisplayName("Average LLM judge overall >= 2.5 (CDM baseline)")
    void testJudgeScore() {
        double avgScore = results.stream()
                .filter(r -> !r.hasError() && r.llmJudge() != null && r.llmJudge().overall() > 0)
                .mapToInt(r -> r.llmJudge().overall())
                .average()
                .orElse(0.0);

        System.out.printf("Average LLM judge overall: %.1f / 5.0%n", avgScore);
        assertTrue(avgScore >= 2.5,
                String.format("Average judge overall %.1f below 2.5 threshold", avgScore));
    }
}
