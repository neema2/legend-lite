package org.finos.legend.engine.nlq;

/**
 * Factory for creating LLM clients based on environment configuration.
 *
 * Environment variables:
 * - LLM_PROVIDER: "gemini" (default), "openai", "anthropic"
 * - GEMINI_API_KEY: Required for Gemini provider
 * - OPENAI_API_KEY: Required for OpenAI provider (future)
 * - ANTHROPIC_API_KEY: Required for Anthropic provider (future)
 */
public final class LlmClientFactory {

    private LlmClientFactory() {}

    /**
     * Creates an LLM client based on the LLM_PROVIDER env var.
     * Defaults to Gemini if not specified.
     */
    public static LlmClient create() {
        String provider = System.getenv("LLM_PROVIDER");
        if (provider == null || provider.isBlank()) {
            provider = "gemini";
        }

        return switch (provider.toLowerCase()) {
            case "gemini" -> new GeminiClient();
            default -> throw new LlmClient.LlmException(
                    "Unsupported LLM provider: " + provider +
                    ". Supported: gemini. Set LLM_PROVIDER env var.", -1, null);
        };
    }

    /**
     * Creates an LLM client with explicit API key.
     */
    public static LlmClient create(String provider, String apiKey) {
        return switch (provider.toLowerCase()) {
            case "gemini" -> new GeminiClient(apiKey, null);
            default -> throw new LlmClient.LlmException(
                    "Unsupported LLM provider: " + provider, -1, null);
        };
    }
}
