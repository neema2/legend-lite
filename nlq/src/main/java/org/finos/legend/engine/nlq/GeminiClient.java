package org.finos.legend.engine.nlq;

import com.gs.legend.util.Json;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LLM client for Google Gemini API.
 * Uses java.net.http.HttpClient — no external dependencies.
 *
 * Requires GEMINI_API_KEY environment variable.
 */
public class GeminiClient implements LlmClient {

    private static final String DEFAULT_MODEL = "gemini-3-flash-preview";
    private static final String BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private final String apiKey;
    private final String model;
    private final HttpClient httpClient;

    public GeminiClient() {
        this(System.getenv("GEMINI_API_KEY"), DEFAULT_MODEL);
    }

    public GeminiClient(String apiKey, String model) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new LlmException("GEMINI_API_KEY environment variable is not set", -1, null);
        }
        this.apiKey = apiKey;
        this.model = model != null ? model : DEFAULT_MODEL;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
    }

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_BACKOFF_MS = 4000;

    @Override
    public String complete(String systemPrompt, String userMessage) {
        String url = BASE_URL + model + ":generateContent?key=" + apiKey;
        String requestBody = buildRequestBody(systemPrompt, userMessage);

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .timeout(TIMEOUT)
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();

                long t0 = System.nanoTime();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                long ms = (System.nanoTime() - t0) / 1_000_000;
                System.out.printf("      [Gemini] attempt=%d status=%d %dms prompt=%dchars%n",
                        attempt, response.statusCode(), ms, requestBody.length());
                System.out.flush();

                if ((response.statusCode() == 429 || response.statusCode() == 503) && attempt < MAX_RETRIES) {
                    long backoff = INITIAL_BACKOFF_MS * (1L << attempt);
                    System.out.printf("      [Gemini] %d, backing off %ds%n", response.statusCode(), backoff / 1000);
                    System.out.flush();
                    try { Thread.sleep(backoff); } catch (InterruptedException ignored) {}
                    continue;
                }

                if (response.statusCode() != 200) {
                    throw new LlmException(
                            "Gemini API returned " + response.statusCode(),
                            response.statusCode(),
                            response.body());
                }

                return extractText(response.body());
            } catch (LlmException e) {
                throw e;
            } catch (Exception e) {
                if (attempt < MAX_RETRIES) {
                    long backoff = INITIAL_BACKOFF_MS * (1L << attempt);
                    System.out.printf("      [Gemini] timeout/error, backing off %ds: %s%n", backoff / 1000, e.getMessage());
                    System.out.flush();
                    try { Thread.sleep(backoff); } catch (InterruptedException ignored) {}
                    continue;
                }
                throw new LlmException("Gemini API request failed: " + e.getMessage(), e);
            }
        }
        throw new LlmException("Gemini API: max retries exhausted", 429, null);
    }

    @Override
    public String provider() {
        return "gemini";
    }

    @Override
    public String model() {
        return model;
    }

    // ==================== JSON helpers (no external deps) ====================

    private static String buildRequestBody(String systemPrompt, String userMessage) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        // System instruction
        if (systemPrompt != null && !systemPrompt.isBlank()) {
            sb.append("\"system_instruction\":{\"parts\":[{\"text\":\"")
              .append(Json.escape(systemPrompt))
              .append("\"}]},");
        }

        // User message
        sb.append("\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":\"")
          .append(Json.escape(userMessage))
          .append("\"}]}],");

        // Generation config
        sb.append("\"generationConfig\":{\"temperature\":0.1,\"maxOutputTokens\":4096}");

        sb.append("}");
        return sb.toString();
    }

    /**
     * Extracts text from Gemini response JSON.
     * Expected path: candidates[0].content.parts[0].text
     *
     * <p>Still regex-based for now to limit blast radius — commit 6 replaces
     * this with a proper Json.parse walk once all NLQ regex extractors are
     * migrated together.
     */
    private static String extractText(String json) {
        Pattern p = Pattern.compile("\"text\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return Json.unescape(m.group(1));
        }
        throw new LlmException("Could not extract text from Gemini response", -1, json);
    }
}
