package org.finos.legend.engine.nlq;

import org.finos.legend.pure.dsl.definition.PureModelBuilder;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Three-step NLQ-to-Pure pipeline:
 * 1. Semantic Router — identifies the root class
 * 2. Query Planner — builds a structured query plan
 * 3. Pure Generator — generates valid Pure query syntax
 *
 * Each step uses the LLM with focused context from the SemanticIndex.
 */
public class NlqService {

    private static final int DEFAULT_TOP_K = 15;

    private final SemanticIndex index;
    private final PureModelBuilder modelBuilder;
    private final LlmClient llmClient;

    public NlqService(SemanticIndex index, PureModelBuilder modelBuilder, LlmClient llmClient) {
        this.index = index;
        this.modelBuilder = modelBuilder;
        this.llmClient = llmClient;
    }

    /**
     * Runs the full NLQ-to-Pure pipeline.
     *
     * @param question The natural language question
     * @param domain   Optional domain hint (e.g., "PnL", "Trading")
     * @return The NlqResult with the generated Pure query
     */
    public NlqResult process(String question, String domain) {
        long start = System.nanoTime();

        try {
            // Step 0: Retrieve relevant classes
            List<SemanticIndex.RetrievalResult> retrieved = index.retrieve(question, DEFAULT_TOP_K, domain);
            Set<String> classNames = retrieved.stream()
                    .map(SemanticIndex.RetrievalResult::qualifiedName)
                    .collect(Collectors.toSet());
            List<String> retrievedList = retrieved.stream()
                    .map(r -> simpleName(r.qualifiedName()))
                    .toList();

            String schema = ModelSchemaExtractor.extractSchema(classNames, modelBuilder);

            // Step 1: Semantic Router — identify root class
            String rootClass = routeToRootClass(question, schema);

            // Step 2: Query Planner — build structured plan
            String queryPlan = planQuery(question, rootClass, schema);

            // Step 3: Pure Generator — generate Pure syntax
            String pureQuery = generatePure(question, rootClass, queryPlan, schema);

            long elapsed = (System.nanoTime() - start) / 1_000_000;

            return new NlqResult(
                    rootClass,
                    queryPlan,
                    pureQuery,
                    "Generated Pure query for " + rootClass,
                    true,
                    null,
                    retrievedList,
                    elapsed
            );
        } catch (Exception e) {
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            return NlqResult.error(e.getMessage(), List.of(), elapsed);
        }
    }

    // ==================== Step 1: Semantic Router ====================

    private String routeToRootClass(String question, String schema) {
        String systemPrompt = """
                You are a data model expert. Given a data model schema and a natural language question,
                identify which class is the PRIMARY entity (root class) that the query should start from.
                
                The root class is the main entity being queried — the one that appears after ".all()" in a Pure query.
                For example, if someone asks "total PnL by trader", the root class is DailyPnL (not Trader),
                because we're querying PnL records and navigating to Trader via association.
                
                Return ONLY a JSON object with this exact format (no markdown, no explanation):
                {"rootClass": "ClassName", "reasoning": "brief explanation"}
                """;

        String userMessage = "Data Model:\n" + schema + "\n\nQuestion: " + question;

        String response = llmClient.complete(systemPrompt, userMessage);
        return extractJsonField(response, "rootClass");
    }

    // ==================== Step 2: Query Planner ====================

    private String planQuery(String question, String rootClass, String schema) {
        String systemPrompt = """
                You are a Pure language query planner. Given a data model, a root class, and a natural language question,
                produce a structured query plan as JSON.
                
                The plan should specify:
                - projections: list of property paths to select (e.g., ["trader.name", "totalPnL"])
                - filters: list of filter conditions with path, operator, and value
                - groupBy: list of property paths to group by (if aggregation needed)
                - aggregations: list of {function, property, alias} objects
                - sort: list of {column, direction} objects
                - limit: optional row limit
                
                Navigation uses dot notation through associations (e.g., "desk.name", "trader.badge").
                Available operators: ==, !=, >, <, >=, <=, contains, in
                Available aggregation functions: sum, avg, count, min, max
                
                Return ONLY valid JSON (no markdown, no explanation).
                """;

        String userMessage = "Root Class: " + rootClass + "\n\nData Model:\n" + schema +
                "\n\nQuestion: " + question;

        return llmClient.complete(systemPrompt, userMessage);
    }

    // ==================== Step 3: Pure Generator ====================

    private String generatePure(String question, String rootClass, String queryPlan, String schema) {
        String systemPrompt = """
                You are a Pure language code generator. Generate a valid Pure query expression.
                
                Pure query syntax examples:
                
                1. Simple filter:
                   Trade.all()->filter(t|$t.status == 'NEW')
                
                2. Filter with navigation:
                   Trade.all()->filter(t|$t.desk.name == 'AMER Equity Swaps')
                
                3. Project columns:
                   Trade.all()->project([t|$t.tradeId, t|$t.notional, t|$t.counterparty.name], ['Trade ID', 'Notional', 'Counterparty'])
                
                4. GroupBy with aggregation:
                   DailyPnL.all()->groupBy([p|$p.trader.name], [agg(p|$p.totalPnL, x|$x->sum())], ['Trader', 'Total PnL'])
                
                5. Sort and limit:
                   Trade.all()->sortBy(t|$t.notional->descending())->limit(10)
                
                6. Combined:
                   DailyPnL.all()->filter(p|$p.desk.name == 'AMER Equity Swaps' && $p.pnlDate >= %2026-01-01)->groupBy([p|$p.trader.name], [agg(p|$p.totalPnL, x|$x->sum())], ['Trader', 'Total PnL'])
                
                Key rules:
                - Always start with ClassName.all()
                - Use $variable references inside lambdas
                - Date literals use %YYYY-MM-DD format
                - Navigate associations using dot notation
                - groupBy takes 3 args: [group lambdas], [agg expressions], ['column names']
                
                Return ONLY the Pure query expression. No explanation, no markdown.
                """;

        String userMessage = "Root Class: " + rootClass +
                "\n\nQuery Plan:\n" + queryPlan +
                "\n\nData Model:\n" + schema +
                "\n\nQuestion: " + question;

        String response = llmClient.complete(systemPrompt, userMessage);

        // Clean up: remove markdown code fences if present
        response = response.strip();
        if (response.startsWith("```")) {
            response = response.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
        }

        return response;
    }

    // ==================== Helpers ====================

    private static String extractJsonField(String json, String field) {
        // Handle both with and without markdown code fences
        String cleaned = json.strip();
        if (cleaned.startsWith("```")) {
            cleaned = cleaned.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
        }

        Pattern p = Pattern.compile("\"" + field + "\"\\s*:\\s*\"([^\"]*?)\"");
        Matcher m = p.matcher(cleaned);
        if (m.find()) {
            return m.group(1);
        }
        throw new LlmClient.LlmException(
                "Could not extract '" + field + "' from LLM response: " + json, -1, json);
    }

    private static String simpleName(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
}
