package org.finos.legend.engine.nlq.eval;

import org.finos.legend.engine.nlq.LlmClient;
import org.finos.legend.engine.nlq.ModelSchemaExtractor;
import org.finos.legend.engine.nlq.NlqResult;
import org.finos.legend.engine.nlq.NlqService;
import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Orchestrates NLQ evaluation across a set of test cases.
 * Runs retrieval scoring against a built SemanticIndex.
 */
public class NlqEvalRunner {

    private final SemanticIndex index;
    private final PureModelBuilder modelBuilder;

    public NlqEvalRunner(SemanticIndex index, PureModelBuilder modelBuilder) {
        this.index = Objects.requireNonNull(index);
        this.modelBuilder = Objects.requireNonNull(modelBuilder);
    }

    public PureModelBuilder getModelBuilder() {
        return modelBuilder;
    }

    /**
     * Loads eval cases from a JSON resource on the classpath.
     * Uses a simple regex-based parser to avoid external JSON dependencies.
     */
    public static List<NlqEvalCase> loadCases(String resourcePath) {
        try (InputStream is = NlqEvalRunner.class.getResourceAsStream(resourcePath)) {
            Objects.requireNonNull(is, "Resource not found: " + resourcePath);
            String json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return parseEvalCases(json);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load eval cases from " + resourcePath, e);
        }
    }

    /**
     * Runs retrieval evaluation for all cases.
     *
     * @param cases The eval cases to run
     * @param topK  The top-K parameter for retrieval
     * @return List of scored results, one per case
     */
    public List<NlqEvalResult> runRetrievalEval(List<NlqEvalCase> cases, int topK) {
        List<NlqEvalResult> results = new ArrayList<>();

        for (NlqEvalCase evalCase : cases) {
            results.add(runSingleRetrieval(evalCase, topK));
        }

        return results;
    }

    /**
     * Runs retrieval evaluation for a single case.
     */
    public NlqEvalResult runSingleRetrieval(NlqEvalCase evalCase, int topK) {
        try {
            long start = System.nanoTime();

            List<SemanticIndex.RetrievalResult> retrieved =
                    index.retrieve(evalCase.question(), topK);

            long elapsed = (System.nanoTime() - start) / 1_000_000;

            NlqEvalResult.RetrievalScore score =
                    NlqEvalMetrics.scoreRetrieval(retrieved, evalCase.expected().retrieval());

            return new NlqEvalResult(
                    evalCase.id(),
                    evalCase.question(),
                    score,
                    elapsed,
                    null
            );
        } catch (Exception e) {
            return new NlqEvalResult(
                    evalCase.id(),
                    evalCase.question(),
                    null,
                    0,
                    e.getMessage()
            );
        }
    }

    // ==================== Full Pipeline Eval ====================

    /**
     * Runs full pipeline evaluation for all cases using a real LLM.
     */
    public List<NlqFullEvalResult> runFullPipelineEval(
            List<NlqEvalCase> cases, NlqService service, LlmClient judge) {

        // Build schema once for the judge
        List<SemanticIndex.RetrievalResult> allClasses = index.retrieve("", 100);
        Set<String> allClassNames = allClasses.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .collect(Collectors.toSet());
        String fullSchema = ModelSchemaExtractor.extractSchema(allClassNames, modelBuilder);

        List<NlqFullEvalResult> results = new ArrayList<>();
        for (int i = 0; i < cases.size(); i++) {
            NlqEvalCase evalCase = cases.get(i);
            System.out.printf("[%d/%d] %s: \"%s\"...%n", i + 1, cases.size(), evalCase.id(), evalCase.question());
            System.out.flush();
            long caseStart = System.nanoTime();
            NlqFullEvalResult result = runSingleFullPipelineEval(evalCase, service, judge, fullSchema);
            long caseMs = (System.nanoTime() - caseStart) / 1_000_000;
            results.add(result);
            System.out.printf("  → %s (%d ms)%s%n", result.error() != null ? "ERROR: " + result.error() : "done", caseMs, result.error() != null ? "" : "");
            System.out.flush();

            // Rate-limit: pause between cases to avoid API throttling (4 LLM calls per case)
            if (i < cases.size() - 1) {
                try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            }
        }
        return results;
    }

    /**
     * Runs full pipeline evaluation for a single case.
     */
    public NlqFullEvalResult runSingleFullPipelineEval(
            NlqEvalCase evalCase, NlqService service, LlmClient judge, String schema) {

        try {
            long start = System.nanoTime();

            // Run retrieval scoring
            NlqEvalResult.RetrievalScore retrievalScore =
                    NlqEvalMetrics.scoreRetrieval(
                            index.retrieve(evalCase.question(), 15),
                            evalCase.expected().retrieval());

            // Run full pipeline
            System.out.print("    pipeline...");
            System.out.flush();
            NlqResult nlqResult = service.process(evalCase.question(), null);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            System.out.printf(" root=%s (%d ms)%n", nlqResult.rootClass(), elapsed);
            System.out.flush();

            if (!nlqResult.isValid()) {
                return NlqFullEvalResult.error(
                        evalCase.id(), evalCase.question(), evalCase.difficulty(),
                        nlqResult.validationError());
            }

            // Score routing (preferred + acceptable)
            NlqFullEvalResult.RoutingScore routing =
                    NlqEvalMetrics.scoreRouting(
                            evalCase.expected().rootClass(),
                            evalCase.expected().acceptableRootClasses(),
                            nlqResult.rootClass());

            // Score query accuracy (deterministic ops)
            NlqFullEvalResult.QueryAccuracyScore queryAccuracy =
                    NlqEvalMetrics.scoreQueryAccuracy(nlqResult.pureQuery(), evalCase.expected().query());

            // Score property roles (deterministic)
            NlqFullEvalResult.PropertyRoleScore propertyRoles =
                    NlqEvalMetrics.scorePropertyRoles(nlqResult.pureQuery(), evalCase.expected().query().properties());

            // LLM-as-judge (structured multi-dimension)
            System.out.print("    judging...");
            System.out.flush();
            NlqFullEvalResult.LlmJudgeScore judgeScore =
                    NlqEvalMetrics.judgeQuery(judge, evalCase.question(), schema,
                            nlqResult.pureQuery(), evalCase.expected().query().referenceQuery());

            System.out.printf(" overall=%d (c%d/f%d/a%d/s%d)%n",
                    judgeScore.overall(), judgeScore.columnSelection(),
                    judgeScore.filtering(), judgeScore.aggregation(),
                    judgeScore.semanticEquivalence());
            System.out.flush();

            return new NlqFullEvalResult(
                    evalCase.id(),
                    evalCase.question(),
                    evalCase.difficulty(),
                    retrievalScore,
                    routing,
                    queryAccuracy,
                    propertyRoles,
                    judgeScore,
                    nlqResult.pureQuery(),
                    evalCase.expected().query().referenceQuery(),
                    elapsed,
                    null
            );
        } catch (Exception e) {
            return NlqFullEvalResult.error(
                    evalCase.id(), evalCase.question(), evalCase.difficulty(), e.getMessage());
        }
    }

    // ==================== Simple JSON Parser ====================

    static List<NlqEvalCase> parseEvalCases(String json) {
        List<NlqEvalCase> cases = new ArrayList<>();

        List<String> caseBlocks = splitJsonObjects(json);

        for (String block : caseBlocks) {
            String id = extractString(block, "id");
            String question = extractString(block, "question");
            String subdomain = extractString(block, "subdomain");
            String difficulty = extractString(block, "difficulty");
            String rootClass = extractString(block, "rootClass");

            List<String> acceptableRootClasses = extractStringArray(block, "acceptableRootClasses");
            if (acceptableRootClasses.isEmpty() && rootClass != null) {
                acceptableRootClasses = List.of(rootClass);
            }

            List<String> mustInclude = extractStringArray(block, "mustInclude");
            List<String> mustExclude = extractStringArray(block, "mustExclude");
            int maxK = extractInt(block, "maxK", 15);

            NlqEvalCase.RetrievalExpectation retrieval =
                    new NlqEvalCase.RetrievalExpectation(mustInclude, mustExclude, maxK);

            String referenceQuery = extractString(block, "referenceQuery");
            List<String> mustContainOps = extractStringArray(block, "mustContainOps");

            // Parse role-based properties
            NlqEvalCase.PropertyRoles properties = parsePropertyRoles(block);

            NlqEvalCase.QueryExpectation query =
                    new NlqEvalCase.QueryExpectation(referenceQuery, mustContainOps, properties);

            NlqEvalCase.ExpectedOutcome expected =
                    new NlqEvalCase.ExpectedOutcome(retrieval, rootClass, acceptableRootClasses, query);

            cases.add(new NlqEvalCase(id, question, subdomain, difficulty, expected));
        }

        return cases;
    }

    private static NlqEvalCase.PropertyRoles parsePropertyRoles(String block) {
        // Extract the "properties" object using brace matching
        String propsBlock = extractNestedObject(block, "properties");
        if (propsBlock == null) {
            return NlqEvalCase.PropertyRoles.EMPTY;
        }

        List<String> dimensions = extractStringArray(propsBlock, "dimensions");
        List<String> filters = extractStringArray(propsBlock, "filters");
        List<String> sortedBy = extractStringArray(propsBlock, "sortedBy");

        // Parse metrics: array of {"property": "...", "function": "..."} objects
        List<NlqEvalCase.MetricExpectation> metrics = new ArrayList<>();
        String metricsBlock = extractNestedArray(propsBlock, "metrics");
        if (metricsBlock != null) {
            List<String> metricObjects = splitJsonObjects(metricsBlock);
            for (String mo : metricObjects) {
                String prop = extractString(mo, "property");
                String func = extractString(mo, "function");
                if (prop != null && func != null) {
                    metrics.add(new NlqEvalCase.MetricExpectation(prop, func));
                }
            }
        }

        return new NlqEvalCase.PropertyRoles(dimensions, metrics, filters, sortedBy);
    }

    private static String extractNestedObject(String json, String key) {
        int keyIdx = json.indexOf("\"" + key + "\"");
        if (keyIdx < 0) return null;

        int colonIdx = json.indexOf(':', keyIdx + key.length() + 2);
        if (colonIdx < 0) return null;

        // Find the opening brace
        int braceStart = json.indexOf('{', colonIdx);
        if (braceStart < 0) return null;

        // Match braces
        int depth = 0;
        for (int i = braceStart; i < json.length(); i++) {
            if (json.charAt(i) == '{') depth++;
            else if (json.charAt(i) == '}') {
                depth--;
                if (depth == 0) return json.substring(braceStart, i + 1);
            }
        }
        return null;
    }

    private static String extractNestedArray(String json, String key) {
        int keyIdx = json.indexOf("\"" + key + "\"");
        if (keyIdx < 0) return null;

        int colonIdx = json.indexOf(':', keyIdx + key.length() + 2);
        if (colonIdx < 0) return null;

        int bracketStart = json.indexOf('[', colonIdx);
        if (bracketStart < 0) return null;

        int depth = 0;
        for (int i = bracketStart; i < json.length(); i++) {
            if (json.charAt(i) == '[') depth++;
            else if (json.charAt(i) == ']') {
                depth--;
                if (depth == 0) return json.substring(bracketStart, i + 1);
            }
        }
        return null;
    }

    private static List<String> splitJsonObjects(String json) {
        List<String> objects = new ArrayList<>();
        int depth = 0;
        int start = -1;

        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{') {
                if (depth == 0) start = i;
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0 && start >= 0) {
                    objects.add(json.substring(start, i + 1));
                    start = -1;
                }
            }
        }

        return objects;
    }

    private static String extractString(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]*?)\"");
        Matcher m = p.matcher(json);
        return m.find() ? m.group(1) : null;
    }

    private static List<String> extractStringArray(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*\\[([^\\]]*?)\\]");
        Matcher m = p.matcher(json);
        if (!m.find()) return List.of();

        String arrayContent = m.group(1);
        List<String> items = new ArrayList<>();
        Matcher itemMatcher = Pattern.compile("\"([^\"]*?)\"").matcher(arrayContent);
        while (itemMatcher.find()) {
            items.add(itemMatcher.group(1));
        }
        return items;
    }

    private static int extractInt(String json, String key, int defaultValue) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*(\\d+)");
        Matcher m = p.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : defaultValue;
    }
}
