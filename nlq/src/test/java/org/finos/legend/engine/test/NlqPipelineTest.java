package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.*;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the NLQ pipeline (NlqService) using MockLlmClient.
 * No real LLM API key required.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NlqPipelineTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws Exception {
        InputStream is = NlqPipelineTest.class.getResourceAsStream("/nlq/sales-trading-model.pure");
        assertNotNull(is, "Test model not found");
        String pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    // ==================== Pipeline Tests ====================

    @Test
    @Order(1)
    @DisplayName("Full pipeline: 3 LLM calls for routing, planning, generating")
    void testFullPipeline() {
        MockLlmClient mock = MockLlmClient.withResponses(
                // Step 1: Router response
                "{\"rootClass\": \"Trade\", \"reasoning\": \"querying trades\"}",
                // Step 2: Planner response
                "{\"projections\": [\"tradeId\", \"notional\"], \"filters\": [{\"path\": \"status\", \"op\": \"==\", \"value\": \"NEW\"}]}",
                // Step 3: Generator response
                "Trade.all()->filter(t|$t.status == 'NEW')->project([t|$t.tradeId, t|$t.notional], ['Trade ID', 'Notional'])"
        );

        NlqService service = new NlqService(index, modelBuilder, mock);
        NlqResult result = service.process("show me new trades", null);

        assertTrue(result.isValid(), "Pipeline should succeed: " + result.validationError());
        assertEquals("Trade", result.rootClass());
        assertNotNull(result.pureQuery());
        assertTrue(result.pureQuery().contains("Trade.all()"));
        assertNotNull(result.retrievedClasses());
        assertFalse(result.retrievedClasses().isEmpty());
        assertTrue(result.latencyMs() >= 0);
        assertEquals(3, mock.callCount(), "Should make exactly 3 LLM calls");
    }

    @Test
    @Order(2)
    @DisplayName("Pipeline returns error when router fails")
    void testRouterFailure() {
        MockLlmClient mock = MockLlmClient.withResponses(
                // Router returns garbage
                "I don't understand the question"
        );

        NlqService service = new NlqService(index, modelBuilder, mock);
        NlqResult result = service.process("show me trades", null);

        assertFalse(result.isValid());
        assertNotNull(result.validationError());
        assertTrue(result.validationError().contains("rootClass"),
                "Error should mention rootClass extraction: " + result.validationError());
    }

    @Test
    @Order(3)
    @DisplayName("Pipeline with domain hint narrows retrieval")
    void testDomainHint() {
        MockLlmClient mock = MockLlmClient.withResponses(
                "{\"rootClass\": \"DailyPnL\", \"reasoning\": \"PnL query\"}",
                "{\"projections\": [\"totalPnL\"]}",
                "DailyPnL.all()->project([p|$p.totalPnL], ['Total PnL'])"
        );

        NlqService service = new NlqService(index, modelBuilder, mock);
        NlqResult result = service.process("total PnL", "PnL");

        assertTrue(result.isValid());
        assertEquals("DailyPnL", result.rootClass());
    }

    @Test
    @Order(4)
    @DisplayName("Pipeline strips markdown code fences from LLM output")
    void testCodeFenceStripping() {
        MockLlmClient mock = MockLlmClient.withResponses(
                "```json\n{\"rootClass\": \"Trade\", \"reasoning\": \"trades\"}\n```",
                "```json\n{\"projections\": [\"tradeId\"]}\n```",
                "```pure\nTrade.all()->project([t|$t.tradeId], ['ID'])\n```"
        );

        NlqService service = new NlqService(index, modelBuilder, mock);
        NlqResult result = service.process("show trades", null);

        assertTrue(result.isValid());
        assertEquals("Trade", result.rootClass());
        assertFalse(result.pureQuery().contains("```"), "Code fences should be stripped");
        assertTrue(result.pureQuery().startsWith("Trade.all()"));
    }

    @Test
    @Order(5)
    @DisplayName("Retrieved classes list is populated")
    void testRetrievedClassesList() {
        MockLlmClient mock = MockLlmClient.withResponses(
                "{\"rootClass\": \"VaRResult\", \"reasoning\": \"risk query\"}",
                "{\"projections\": [\"var95\"]}",
                "VaRResult.all()->project([v|$v.var95], ['VaR'])"
        );

        NlqService service = new NlqService(index, modelBuilder, mock);
        NlqResult result = service.process("VaR by portfolio", null);

        assertTrue(result.isValid());
        assertTrue(result.retrievedClasses().contains("VaRResult"),
                "Should include VaRResult: " + result.retrievedClasses());
        assertTrue(result.retrievedClasses().contains("Portfolio"),
                "Should include Portfolio: " + result.retrievedClasses());
    }

    // ==================== MockLlmClient Tests ====================

    @Test
    @Order(10)
    @DisplayName("MockLlmClient.withResponses returns canned responses in order")
    void testMockResponses() {
        MockLlmClient mock = MockLlmClient.withResponses("first", "second", "third");

        assertEquals("first", mock.complete("sys", "msg1"));
        assertEquals("second", mock.complete("sys", "msg2"));
        assertEquals("third", mock.complete("sys", "msg3"));
        assertEquals(3, mock.callCount());
    }

    @Test
    @Order(11)
    @DisplayName("MockLlmClient throws when responses exhausted")
    void testMockExhausted() {
        MockLlmClient mock = MockLlmClient.withResponses("only one");
        mock.complete("sys", "msg");

        assertThrows(LlmClient.LlmException.class, () -> mock.complete("sys", "msg2"));
    }

    // ==================== GeminiClient JSON Helpers ====================

    @Test
    @Order(20)
    @DisplayName("GeminiClient.escapeJson handles special characters")
    void testEscapeJson() {
        assertEquals("\"hello\"", GeminiClient.escapeJson("hello"));
        assertEquals("\"line1\\nline2\"", GeminiClient.escapeJson("line1\nline2"));
        assertEquals("\"tab\\there\"", GeminiClient.escapeJson("tab\there"));
        assertEquals("\"quote\\\"here\"", GeminiClient.escapeJson("quote\"here"));
        assertEquals("\"back\\\\slash\"", GeminiClient.escapeJson("back\\slash"));
    }

    @Test
    @Order(21)
    @DisplayName("GeminiClient.escapeJson handles empty and simple strings")
    void testEscapeJsonSimple() {
        assertEquals("\"\"", GeminiClient.escapeJson(""));
        assertEquals("\"abc123\"", GeminiClient.escapeJson("abc123"));
    }

    // ==================== NlqResult Tests ====================

    @Test
    @Order(30)
    @DisplayName("NlqResult.error creates error result")
    void testNlqResultError() {
        NlqResult result = NlqResult.error("something broke", List.of("Trade"), 42);

        assertFalse(result.isValid());
        assertNull(result.rootClass());
        assertNull(result.pureQuery());
        assertEquals("something broke", result.validationError());
        assertEquals(List.of("Trade"), result.retrievedClasses());
        assertEquals(42, result.latencyMs());
    }
}
