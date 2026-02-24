package org.finos.legend.engine.nlq;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.finos.legend.engine.server.LegendHttpServer;
import org.finos.legend.engine.server.LegendHttpJson;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Extends the base Legend HTTP server with the NLQ endpoint.
 *
 * Run with:
 * GEMINI_API_KEY=... java -cp nlq/target/classes:engine/target/classes \
 * org.finos.legend.engine.nlq.NlqHttpServer [port]
 */
public class NlqHttpServer {

    public static void main(String[] args) throws IOException {
        int port = 8080;
        String envPort = System.getenv("PORT");
        if (envPort != null && !envPort.isBlank()) {
            try {
                port = Integer.parseInt(envPort);
            } catch (NumberFormatException e) {
                System.err.println("Invalid PORT env var: " + envPort);
            }
        }
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0]);
            }
        }

        LegendHttpServer server = new LegendHttpServer(port);
        server.addContext("/engine/nlq", new NlqHandler());
        server.start();

        System.out.println();
        System.out.println("======================================");
        System.out.println("  Legend Studio Lite - Backend Ready");
        System.out.println("  (NLQ enabled)");
        System.out.println("======================================");
        System.out.println();
        System.out.println("Endpoints:");
        System.out.println("  POST http://localhost:" + port + "/lsp             - LSP Protocol");
        System.out.println("  POST http://localhost:" + port + "/engine/execute   - Execute Pure query");
        System.out.println("  POST http://localhost:" + port + "/engine/sql       - Execute raw SQL");
        System.out.println("  POST http://localhost:" + port + "/engine/nlq       - NLQ to Pure query");
        System.out.println("  GET  http://localhost:" + port + "/health           - Health check");
        System.out.println();
        System.out.println("Press Ctrl+C to stop");

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }

    /**
     * NLQ - Natural Language Query to Pure.
     *
     * Request format:
     * {
     * "code": "full Pure model source",
     * "question": "show me total PnL by trader",
     * "domain": "PnL" // optional hint
     * }
     */
    static class NlqHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            LegendHttpServer.addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                LegendHttpServer.sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            try {
                String body = LegendHttpServer.readBody(exchange);
                Map<String, Object> request = LegendHttpJson.parseObject(body);
                String pureSource = LegendHttpJson.getString(request, "code");
                String question = LegendHttpJson.getString(request, "question");
                String domain = LegendHttpJson.getString(request, "domain");

                if (pureSource == null || pureSource.isBlank()) {
                    LegendHttpServer.sendResponse(exchange, 400, "{\"error\":\"Missing 'code' field\"}");
                    return;
                }
                if (question == null || question.isBlank()) {
                    LegendHttpServer.sendResponse(exchange, 400, "{\"error\":\"Missing 'question' field\"}");
                    return;
                }

                // Build model and index
                PureModelBuilder modelBuilder = new PureModelBuilder();
                modelBuilder.addSource(pureSource);

                SemanticIndex index = new SemanticIndex();
                index.buildIndex(modelBuilder);

                // Create LLM client
                LlmClient llmClient = LlmClientFactory.create();

                // Run NLQ pipeline
                NlqService nlqService = new NlqService(index, modelBuilder, llmClient);
                NlqResult result = nlqService.process(question, domain);

                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", result.isValid());
                response.put("rootClass", result.rootClass());
                response.put("pureQuery", result.pureQuery());
                response.put("explanation", result.explanation());
                response.put("queryPlan", result.queryPlan());
                response.put("retrievedClasses", result.retrievedClasses());
                response.put("latencyMs", result.latencyMs());

                if (result.validationError() != null) {
                    response.put("error", result.validationError());
                }

                LegendHttpServer.sendResponse(exchange, 200, LegendHttpJson.toJson(response));

            } catch (LlmClient.LlmException e) {
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", false);
                response.put("error", "LLM Error: " + e.getMessage());
                if (e.statusCode() > 0) {
                    response.put("statusCode", e.statusCode());
                }
                LegendHttpServer.sendResponse(exchange, 200, LegendHttpJson.toJson(response));
            } catch (Exception e) {
                e.printStackTrace();
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", false);
                response.put("error", e.getMessage());
                LegendHttpServer.sendResponse(exchange, 200, LegendHttpJson.toJson(response));
            }
        }
    }
}
