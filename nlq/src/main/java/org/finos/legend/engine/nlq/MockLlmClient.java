package org.finos.legend.engine.nlq;

import java.util.function.BiFunction;

/**
 * Mock LLM client for testing the NLQ pipeline without a real API key.
 * Accepts a lambda that maps (systemPrompt, userMessage) → response.
 */
public class MockLlmClient implements LlmClient {

    private final BiFunction<String, String, String> handler;
    private int callCount = 0;

    /**
     * Creates a mock that returns responses based on the handler function.
     */
    public MockLlmClient(BiFunction<String, String, String> handler) {
        this.handler = handler;
    }

    /**
     * Creates a mock that returns canned responses in order.
     * Call 1 → responses[0], Call 2 → responses[1], etc.
     */
    public static MockLlmClient withResponses(String... responses) {
        int[] idx = {0};
        return new MockLlmClient((sys, user) -> {
            if (idx[0] < responses.length) {
                return responses[idx[0]++];
            }
            throw new LlmException("MockLlmClient: no more canned responses", -1, null);
        });
    }

    @Override
    public String complete(String systemPrompt, String userMessage) {
        callCount++;
        return handler.apply(systemPrompt, userMessage);
    }

    @Override
    public String provider() {
        return "mock";
    }

    @Override
    public String model() {
        return "mock-v1";
    }

    public int callCount() {
        return callCount;
    }
}
