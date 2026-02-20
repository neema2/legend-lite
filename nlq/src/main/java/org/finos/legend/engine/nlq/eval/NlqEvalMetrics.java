package org.finos.legend.engine.nlq.eval;

import org.finos.legend.engine.nlq.LlmClient;
import org.finos.legend.engine.nlq.SemanticIndex;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scoring functions for NLQ evaluation.
 */
public final class NlqEvalMetrics {

    private NlqEvalMetrics() {}

    /**
     * Scores retrieval results against expected outcomes.
     *
     * @param retrieved  The retrieval results from the SemanticIndex
     * @param expected   The expected retrieval outcome from the eval case
     * @return A scored RetrievalScore
     */
    public static NlqEvalResult.RetrievalScore scoreRetrieval(
            List<SemanticIndex.RetrievalResult> retrieved,
            NlqEvalCase.RetrievalExpectation expected) {

        // Extract simple class names from qualified names
        List<String> retrievedClasses = retrieved.stream()
                .map(r -> simpleName(r.qualifiedName()))
                .toList();

        Set<String> retrievedSet = new HashSet<>(retrievedClasses);

        // Must-include check
        Set<String> missingClasses = new LinkedHashSet<>();
        for (String must : expected.mustInclude()) {
            if (!retrievedSet.contains(must)) {
                missingClasses.add(must);
            }
        }
        boolean allMustInclude = missingClasses.isEmpty();

        // Recall = fraction of mustInclude that were retrieved
        double recall = expected.mustInclude().isEmpty() ? 1.0
                : (double) (expected.mustInclude().size() - missingClasses.size())
                  / expected.mustInclude().size();

        // Must-exclude check
        Set<String> unexpectedClasses = new LinkedHashSet<>();
        for (String exclude : expected.mustExclude()) {
            if (retrievedSet.contains(exclude)) {
                unexpectedClasses.add(exclude);
            }
        }
        boolean allMustExclude = unexpectedClasses.isEmpty();

        // Exclusion rate = fraction of mustExclude that were NOT retrieved
        double exclusionRate = expected.mustExclude().isEmpty() ? 1.0
                : (double) (expected.mustExclude().size() - unexpectedClasses.size())
                  / expected.mustExclude().size();

        return new NlqEvalResult.RetrievalScore(
                recall,
                exclusionRate,
                allMustInclude,
                allMustExclude,
                retrievedClasses,
                missingClasses,
                unexpectedClasses
        );
    }

    /**
     * Generates a summary report from a list of eval results.
     */
    public static String generateReport(List<NlqEvalResult> results) {
        StringBuilder sb = new StringBuilder();

        int total = results.size();
        int passed = 0;
        int failed = 0;
        int errors = 0;
        double totalRecall = 0;
        double totalExclusion = 0;
        long totalLatency = 0;

        for (NlqEvalResult r : results) {
            if (r.hasError()) {
                errors++;
            } else if (r.retrieval().passed()) {
                passed++;
            } else {
                failed++;
            }

            if (r.retrieval() != null) {
                totalRecall += r.retrieval().recall();
                totalExclusion += r.retrieval().exclusionRate();
            }
            totalLatency += r.latencyMs();
        }

        sb.append("═══════════════════════════════════════════════════\n");
        sb.append("  NLQ Retrieval Eval Report\n");
        sb.append(String.format("  Cases: %d | Passed: %d | Failed: %d | Errors: %d\n",
                total, passed, failed, errors));
        sb.append("═══════════════════════════════════════════════════\n\n");

        sb.append(String.format("  Avg Recall:      %.1f%%\n", 100.0 * totalRecall / Math.max(1, total)));
        sb.append(String.format("  Avg Exclusion:   %.1f%%\n", 100.0 * totalExclusion / Math.max(1, total)));
        sb.append(String.format("  Avg Latency:     %d ms\n", totalLatency / Math.max(1, total)));
        sb.append(String.format("  Pass Rate:       %.1f%%\n\n", 100.0 * passed / Math.max(1, total)));

        // Failed cases detail
        List<NlqEvalResult> failures = results.stream()
                .filter(r -> !r.hasError() && r.retrieval() != null && !r.retrieval().passed())
                .toList();

        if (!failures.isEmpty()) {
            sb.append("  Failures:\n");
            for (NlqEvalResult f : failures) {
                sb.append(String.format("    %s: \"%s\"\n", f.caseId(), f.question()));
                if (!f.retrieval().missingClasses().isEmpty()) {
                    sb.append("      Missing: ").append(f.retrieval().missingClasses()).append("\n");
                }
                if (!f.retrieval().unexpectedClasses().isEmpty()) {
                    sb.append("      Unexpected: ").append(f.retrieval().unexpectedClasses()).append("\n");
                }
                sb.append("      Retrieved: ").append(f.retrieval().retrievedClasses()).append("\n");
            }
            sb.append("\n");
        }

        // Error cases detail
        List<NlqEvalResult> errorCases = results.stream()
                .filter(NlqEvalResult::hasError)
                .toList();

        if (!errorCases.isEmpty()) {
            sb.append("  Errors:\n");
            for (NlqEvalResult e : errorCases) {
                sb.append(String.format("    %s: %s\n", e.caseId(), e.error()));
            }
            sb.append("\n");
        }

        sb.append("═══════════════════════════════════════════════════\n");
        return sb.toString();
    }

    // ==================== Routing Scoring ====================

    public static NlqFullEvalResult.RoutingScore scoreRouting(String expected, String actual) {
        // Compare simple names — LLM may return "sales::SalesCredit" vs expected "SalesCredit"
        boolean correct = expected != null && actual != null
                && simpleName(expected).equals(simpleName(actual));
        return new NlqFullEvalResult.RoutingScore(expected, actual, correct);
    }

    // ==================== Query Accuracy Scoring ====================

    public static NlqFullEvalResult.QueryAccuracyScore scoreQueryAccuracy(
            String pureQuery, NlqEvalCase.QueryExpectation expected) {

        if (pureQuery == null || pureQuery.isBlank()) {
            return new NlqFullEvalResult.QueryAccuracyScore(
                    false, false, 0.0, 0.0,
                    expected.mustContainOps(), expected.mustReferenceProperties());
        }

        // Check if it starts with ClassName.all()
        boolean parseable = pureQuery.contains(".all()");
        boolean correctRoot = expected.referenceQuery() != null
                && extractRootClass(pureQuery) != null
                && extractRootClass(pureQuery).equals(extractRootClass(expected.referenceQuery()));

        // Op coverage: check for expected operations
        List<String> missingOps = new ArrayList<>();
        for (String op : expected.mustContainOps()) {
            if (!pureQuery.contains(op)) {
                missingOps.add(op);
            }
        }
        double opCoverage = expected.mustContainOps().isEmpty() ? 1.0
                : (double) (expected.mustContainOps().size() - missingOps.size())
                  / expected.mustContainOps().size();

        // Property coverage: check for expected property references
        List<String> missingProperties = new ArrayList<>();
        for (String prop : expected.mustReferenceProperties()) {
            if (!pureQuery.contains(prop)) {
                missingProperties.add(prop);
            }
        }
        double propertyCoverage = expected.mustReferenceProperties().isEmpty() ? 1.0
                : (double) (expected.mustReferenceProperties().size() - missingProperties.size())
                  / expected.mustReferenceProperties().size();

        return new NlqFullEvalResult.QueryAccuracyScore(
                parseable, correctRoot, opCoverage, propertyCoverage,
                missingOps, missingProperties);
    }

    private static String extractRootClass(String query) {
        // Handle both "Trade.all()" and "trading::Trade.all()"
        Pattern p = Pattern.compile("^\\s*([\\w:]+)\\.all\\(\\)");
        Matcher m = p.matcher(query);
        return m.find() ? simpleName(m.group(1)) : null;
    }

    // ==================== LLM-as-Judge Scoring ====================

    public static NlqFullEvalResult.LlmJudgeScore judgeQuery(
            LlmClient judge, String question, String schema,
            String generatedQuery, String referenceQuery) {

        String systemPrompt = """
                You are an expert evaluator of Pure language queries against a data model.
                Given a natural language question, a data model schema, a reference (gold standard) query,
                and a generated query, rate the generated query on a 1-5 scale:
                
                5: Semantically equivalent — would return the same data as the reference
                4: Minor differences (extra columns, slightly different filter) but captures the intent
                3: Correct root class and general approach but missing key elements (wrong groupBy, missing filter)
                2: Wrong approach or significant structural errors
                1: Completely wrong or broken syntax
                
                Return ONLY a JSON object: {"score": N, "reasoning": "brief explanation"}
                """;

        String userMessage = "Question: " + question +
                "\n\nReference Query:\n" + referenceQuery +
                "\n\nGenerated Query:\n" + generatedQuery +
                "\n\nData Model:\n" + schema;

        try {
            String response = judge.complete(systemPrompt, userMessage);
            return parseLlmJudgeResponse(response);
        } catch (Exception e) {
            return new NlqFullEvalResult.LlmJudgeScore(0, "Judge error: " + e.getMessage());
        }
    }

    private static NlqFullEvalResult.LlmJudgeScore parseLlmJudgeResponse(String response) {
        // Strip markdown fences if present
        String cleaned = response.strip();
        if (cleaned.startsWith("```")) {
            cleaned = cleaned.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
        }

        // Extract score
        Pattern scorePat = Pattern.compile("\"score\"\\s*:\\s*(\\d)");
        Matcher sm = scorePat.matcher(cleaned);
        int score = sm.find() ? Integer.parseInt(sm.group(1)) : 0;

        // Extract reasoning
        Pattern reasonPat = Pattern.compile("\"reasoning\"\\s*:\\s*\"([^\"]*?)\"");
        Matcher rm = reasonPat.matcher(cleaned);
        String reasoning = rm.find() ? rm.group(1) : cleaned;

        return new NlqFullEvalResult.LlmJudgeScore(score, reasoning);
    }

    // ==================== Full Pipeline Report ====================

    public static String generateFullReport(List<NlqFullEvalResult> results) {
        StringBuilder sb = new StringBuilder();

        int total = results.size();
        int errors = 0;
        int routingCorrect = 0;
        double totalOpCoverage = 0;
        double totalPropCoverage = 0;
        double totalJudgeScore = 0;
        int judgeCount = 0;
        long totalLatency = 0;

        Map<String, int[]> byDifficulty = new LinkedHashMap<>();
        Map<String, int[]> bySubdomain = new LinkedHashMap<>();

        for (NlqFullEvalResult r : results) {
            if (r.hasError()) {
                errors++;
                continue;
            }

            totalLatency += r.latencyMs();

            if (r.routing() != null && r.routing().correct()) routingCorrect++;
            if (r.queryAccuracy() != null) {
                totalOpCoverage += r.queryAccuracy().opCoverage();
                totalPropCoverage += r.queryAccuracy().propertyCoverage();
            }
            if (r.llmJudge() != null && r.llmJudge().score() > 0) {
                totalJudgeScore += r.llmJudge().score();
                judgeCount++;
            }
        }

        int scored = total - errors;

        sb.append("\n═══════════════════════════════════════════════════════════\n");
        sb.append("  NLQ Full Pipeline Eval Report\n");
        sb.append(String.format("  Cases: %d | Errors: %d\n", total, errors));
        sb.append("═══════════════════════════════════════════════════════════\n\n");

        sb.append(String.format("  Routing Accuracy:      %d/%d (%.1f%%)\n",
                routingCorrect, scored, 100.0 * routingCorrect / Math.max(1, scored)));
        sb.append(String.format("  Avg Op Coverage:       %.1f%%\n", 100.0 * totalOpCoverage / Math.max(1, scored)));
        sb.append(String.format("  Avg Property Coverage: %.1f%%\n", 100.0 * totalPropCoverage / Math.max(1, scored)));
        sb.append(String.format("  Avg LLM Judge Score:   %.1f / 5.0\n",
                totalJudgeScore / Math.max(1, judgeCount)));
        sb.append(String.format("  Avg Latency:           %d ms\n\n", totalLatency / Math.max(1, scored)));

        // By difficulty
        for (NlqFullEvalResult r : results) {
            if (r.hasError()) continue;
            String diff = r.difficulty() != null ? r.difficulty() : "unknown";
            byDifficulty.computeIfAbsent(diff, k -> new int[4]); // [total, routeOk, judgeSum, judgeCount]
            int[] d = byDifficulty.get(diff);
            d[0]++;
            if (r.routing() != null && r.routing().correct()) d[1]++;
            if (r.llmJudge() != null && r.llmJudge().score() > 0) {
                d[2] += r.llmJudge().score();
                d[3]++;
            }
        }

        sb.append("  By Difficulty:\n");
        for (var entry : byDifficulty.entrySet()) {
            int[] d = entry.getValue();
            sb.append(String.format("    %-8s routing %d/%d, judge avg %.1f\n",
                    entry.getKey() + ":", d[1], d[0],
                    d[3] > 0 ? (double) d[2] / d[3] : 0.0));
        }
        sb.append("\n");

        // By subdomain — collect from eval case metadata
        for (NlqFullEvalResult r : results) {
            if (r.hasError()) continue;
            // Use caseId prefix as subdomain proxy
            String sub = r.caseId().replaceAll("-\\d+$", "");
            bySubdomain.computeIfAbsent(sub, k -> new int[4]);
            int[] d = bySubdomain.get(sub);
            d[0]++;
            if (r.routing() != null && r.routing().correct()) d[1]++;
            if (r.llmJudge() != null && r.llmJudge().score() > 0) {
                d[2] += r.llmJudge().score();
                d[3]++;
            }
        }

        sb.append("  By Subdomain:\n");
        for (var entry : bySubdomain.entrySet()) {
            int[] d = entry.getValue();
            sb.append(String.format("    %-10s routing %d/%d, judge avg %.1f\n",
                    entry.getKey() + ":", d[1], d[0],
                    d[3] > 0 ? (double) d[2] / d[3] : 0.0));
        }
        sb.append("\n");

        // Per-case detail
        sb.append("  Per-Case Detail:\n");
        for (NlqFullEvalResult r : results) {
            if (r.hasError()) {
                sb.append(String.format("    ⚠ %s: ERROR — %s\n", r.caseId(), r.error()));
                continue;
            }

            boolean routeOk = r.routing() != null && r.routing().correct();
            String routeStr = routeOk ? "✓" :
                    "✗(expected " + (r.routing() != null ? r.routing().expected() : "?") + ")";

            String opsStr = r.queryAccuracy() != null
                    ? String.format("%.0f%%", r.queryAccuracy().opCoverage() * 100) : "?";
            String propsStr = r.queryAccuracy() != null
                    ? String.format("%.0f%%", r.queryAccuracy().propertyCoverage() * 100) : "?";
            String judgeStr = r.llmJudge() != null && r.llmJudge().score() > 0
                    ? r.llmJudge().score() + "/5" : "?";

            String icon = routeOk && r.llmJudge() != null && r.llmJudge().score() >= 4 ? "✅" : "❌";

            sb.append(String.format("    %s %-12s root=%s %s  ops=%s  props=%s  judge=%s\n",
                    icon, r.caseId() + ":",
                    r.routing() != null ? r.routing().actual() : "?",
                    routeStr, opsStr, propsStr, judgeStr));

            // Show generated query for failures
            if (!routeOk || (r.llmJudge() != null && r.llmJudge().score() < 4)) {
                if (r.generatedQuery() != null) {
                    sb.append("                   Generated: ").append(r.generatedQuery()).append("\n");
                }
                if (r.referenceQuery() != null) {
                    sb.append("                   Reference: ").append(r.referenceQuery()).append("\n");
                }
                if (r.llmJudge() != null && r.llmJudge().reasoning() != null) {
                    sb.append("                   Reason:    ").append(r.llmJudge().reasoning()).append("\n");
                }
            }
        }

        sb.append("\n═══════════════════════════════════════════════════════════\n");
        return sb.toString();
    }

    private static String simpleName(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
}
