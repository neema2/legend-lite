package org.finos.legend.engine.nlq;

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

    private static final String DEFAULT_MODEL = "gemini-2.0-flash";
    private static final String BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

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

                if (response.statusCode() == 429 && attempt < MAX_RETRIES) {
                    long backoff = INITIAL_BACKOFF_MS * (1L << attempt);
                    System.out.printf("      [Gemini] rate-limited, backing off %ds%n", backoff / 1000);
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
            sb.append("\"system_instruction\":{\"parts\":[{\"text\":")
              .append(escapeJson(systemPrompt))
              .append("}]},");
        }

        // User message
        sb.append("\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":")
          .append(escapeJson(userMessage))
          .append("}]}],");

        // Generation config
        sb.append("\"generationConfig\":{\"temperature\":0.1,\"maxOutputTokens\":4096}");

        sb.append("}");
        return sb.toString();
    }

    /**
     * Extracts text from Gemini response JSON.
     * Expected path: candidates[0].content.parts[0].text
     */
    private static String extractText(String json) {
        // Simple regex extraction — robust enough for well-formed Gemini responses
        Pattern p = Pattern.compile("\"text\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return unescapeJson(m.group(1));
        }
        throw new LlmException("Could not extract text from Gemini response", -1, json);
    }

    public static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append("\"");
        return sb.toString();
    }

    private static String unescapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                switch (next) {
                    case 'n' -> { sb.append('\n'); i++; }
                    case 'r' -> { sb.append('\r'); i++; }
                    case 't' -> { sb.append('\t'); i++; }
                    case '"' -> { sb.append('"'); i++; }
                    case '\\' -> { sb.append('\\'); i++; }
                    case '/' -> { sb.append('/'); i++; }
                    case 'u' -> {
                        if (i + 5 < s.length()) {
                            String hex = s.substring(i + 2, i + 6);
                            sb.append((char) Integer.parseInt(hex, 16));
                            i += 5;
                        } else {
                            sb.append(c);
                        }
                    }
                    default -> sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
