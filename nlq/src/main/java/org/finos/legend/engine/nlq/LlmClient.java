package org.finos.legend.engine.nlq;

/**
 * Interface for LLM providers used in the NLQ pipeline.
 * Implementations must handle authentication, rate limiting, and retries.
 */
public interface LlmClient {

    /**
     * Sends a completion request to the LLM.
     *
     * @param systemPrompt The system prompt providing context and instructions
     * @param userMessage  The user's query/message
     * @return The LLM's response text
     * @throws LlmException if the request fails
     */
    String complete(String systemPrompt, String userMessage);

    /**
     * @return The provider name (e.g., "gemini", "openai", "anthropic")
     */
    String provider();

    /**
     * @return The model identifier (e.g., "gemini-2.0-flash", "gpt-4o")
     */
    String model();

    /**
     * Exception wrapper for LLM API errors.
     */
    class LlmException extends RuntimeException {
        private final int statusCode;
        private final String responseBody;

        public LlmException(String message, int statusCode, String responseBody) {
            super(message);
            this.statusCode = statusCode;
            this.responseBody = responseBody;
        }

        public LlmException(String message, Throwable cause) {
            super(message, cause);
            this.statusCode = -1;
            this.responseBody = null;
        }

        public int statusCode() { return statusCode; }
        public String responseBody() { return responseBody; }
    }
}
