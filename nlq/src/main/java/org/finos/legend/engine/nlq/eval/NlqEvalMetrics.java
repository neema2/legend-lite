package org.finos.legend.engine.nlq.eval;

import org.finos.legend.engine.nlq.LlmClient;
import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.pure.dsl.PureParser;

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

    public static NlqFullEvalResult.RoutingScore scoreRouting(
            String preferred, List<String> acceptable, String actual) {
        String actualSimple = actual != null ? simpleName(actual) : null;
        boolean preferredMatch = preferred != null && actualSimple != null
                && simpleName(preferred).equals(actualSimple);
        boolean acceptableMatch = preferredMatch;
        if (!acceptableMatch && actualSimple != null) {
            for (String acc : acceptable) {
                if (simpleName(acc).equals(actualSimple)) {
                    acceptableMatch = true;
                    break;
                }
            }
        }
        return new NlqFullEvalResult.RoutingScore(preferred, acceptable, actual, preferredMatch, acceptableMatch);
    }

    // ==================== Query Accuracy Scoring ====================

    public static NlqFullEvalResult.QueryAccuracyScore scoreQueryAccuracy(
            String pureQuery, NlqEvalCase.QueryExpectation expected) {

        if (pureQuery == null || pureQuery.isBlank()) {
            return new NlqFullEvalResult.QueryAccuracyScore(
                    false, 0.0, expected.mustContainOps());
        }

        boolean parseable;
        try {
            PureParser.parse(pureQuery);
            parseable = true;
        } catch (Exception e) {
            parseable = false;
        }

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

        return new NlqFullEvalResult.QueryAccuracyScore(parseable, opCoverage, missingOps);
    }

    // ==================== Property Role Scoring ====================

    public static NlqFullEvalResult.PropertyRoleScore scorePropertyRoles(
            String pureQuery, NlqEvalCase.PropertyRoles expected) {

        if (pureQuery == null || pureQuery.isBlank() || expected == null) {
            return new NlqFullEvalResult.PropertyRoleScore(0, 0, 0, 0,
                    expected != null ? expected.dimensions() : List.of(),
                    expected != null ? expected.metrics().stream().map(m -> m.property() + ":" + m.function()).toList() : List.of(),
                    expected != null ? expected.filters() : List.of(),
                    expected != null ? expected.sortedBy() : List.of());
        }

        // Extract query sections for role-based checking
        String groupBySection = extractSection(pureQuery, "groupBy");
        String filterSection = extractSection(pureQuery, "filter");
        String sortSection = extractSection(pureQuery, "sort");

        // Dimension coverage: properties in groupBy first arg
        List<String> missingDims = new ArrayList<>();
        for (String dim : expected.dimensions()) {
            String leaf = dim.contains(".") ? dim.substring(dim.lastIndexOf('.') + 1) : dim;
            if (groupBySection == null || (!groupBySection.contains(dim) && !groupBySection.contains(leaf))) {
                missingDims.add(dim);
            }
        }
        double dimCov = expected.dimensions().isEmpty() ? 0.0
                : (double) (expected.dimensions().size() - missingDims.size()) / expected.dimensions().size();

        // Metric coverage: property + aggregation function in groupBy/agg
        List<String> missingMetrics = new ArrayList<>();
        for (NlqEvalCase.MetricExpectation metric : expected.metrics()) {
            String leaf = metric.property().contains(".")
                    ? metric.property().substring(metric.property().lastIndexOf('.') + 1) : metric.property();
            boolean propFound = pureQuery.contains(metric.property()) || pureQuery.contains(leaf);
            boolean funcFound = pureQuery.contains(metric.function());
            if (!propFound || !funcFound) {
                missingMetrics.add(metric.property() + ":" + metric.function());
            }
        }
        double metricCov = expected.metrics().isEmpty() ? 0.0
                : (double) (expected.metrics().size() - missingMetrics.size()) / expected.metrics().size();

        // Filter coverage: properties mentioned in filter context
        List<String> missingFilters = new ArrayList<>();
        for (String filt : expected.filters()) {
            // Strip ~ prefix (column alias marker) for matching
            String lookup = filt.startsWith("~") ? filt.substring(1) : filt;
            boolean found = false;
            if (filterSection != null && (filterSection.contains(lookup) || filterSection.contains(filt))) {
                found = true;
            }
            // Also check full query for class-level filters
            if (!found && pureQuery.contains(lookup)) {
                found = true;
            }
            if (!found) missingFilters.add(filt);
        }
        double filterCov = expected.filters().isEmpty() ? 0.0
                : (double) (expected.filters().size() - missingFilters.size()) / expected.filters().size();

        // Sort coverage
        List<String> missingSorts = new ArrayList<>();
        for (String s : expected.sortedBy()) {
            if (sortSection == null || (!sortSection.contains(s) && !pureQuery.contains("sort") )) {
                missingSorts.add(s);
            }
        }
        double sortCov = expected.sortedBy().isEmpty() ? 0.0
                : (double) (expected.sortedBy().size() - missingSorts.size()) / expected.sortedBy().size();

        return new NlqFullEvalResult.PropertyRoleScore(
                dimCov, metricCov, filterCov, sortCov,
                missingDims, missingMetrics, missingFilters, missingSorts);
    }

    private static String extractSection(String query, String operation) {
        int idx = query.indexOf("->" + operation + "(");
        if (idx < 0) idx = query.indexOf("." + operation + "(");
        if (idx < 0) return null;

        // Extract from operation to next ->  or end
        int start = idx;
        int depth = 0;
        for (int i = query.indexOf('(', idx); i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return query.substring(start, i + 1);
            }
        }
        return query.substring(start);
    }

    // ==================== LLM-as-Judge Scoring ====================

    public static NlqFullEvalResult.LlmJudgeScore judgeQuery(
            LlmClient judge, String question, String schema,
            String generatedQuery, String referenceQuery) {

        String systemPrompt = """
                You are an expert evaluator of Pure language queries against a data model.
                Given a natural language question, a reference query, and a generated query,
                rate the generated query on four dimensions (each 1-5):
                
                IMPORTANT: A query starting from a different but associated class that navigates
                to the correct data via associations is equally valid. Do NOT penalize for choosing
                a different root class if the query reaches the same data.
                
                Dimensions:
                - columnSelection: Are the right columns projected/output? (5=all key columns, 1=none)
                - filtering: Are the right predicates applied? (5=exact, 3=partial, 1=none/wrong)
                - aggregation: Are groupBy dimensions and metric aggregations correct? (5=exact, 3=partial, 1=wrong. Use 5 if N/A)
                - semanticEquivalence: Would this query return essentially the same result set? (5=yes, 1=no)
                
                Also provide an overall score (1-5) and brief reasoning.
                
                Return ONLY a JSON object:
                {"columnSelection": N, "filtering": N, "aggregation": N, "semanticEquivalence": N, "overall": N, "reasoning": "brief explanation"}
                """;

        String userMessage = "Question: " + question +
                "\n\nReference Query:\n" + referenceQuery +
                "\n\nGenerated Query:\n" + generatedQuery +
                "\n\nData Model:\n" + schema;

        try {
            String response = judge.complete(systemPrompt, userMessage);
            return parseLlmJudgeResponse(response);
        } catch (Exception e) {
            return new NlqFullEvalResult.LlmJudgeScore(0, 0, 0, 0, 0, "Judge error: " + e.getMessage());
        }
    }

    private static NlqFullEvalResult.LlmJudgeScore parseLlmJudgeResponse(String response) {
        String cleaned = response.strip();
        if (cleaned.startsWith("```")) {
            cleaned = cleaned.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
        }

        int colSel = extractJsonInt(cleaned, "columnSelection");
        int filtering = extractJsonInt(cleaned, "filtering");
        int aggregation = extractJsonInt(cleaned, "aggregation");
        int semEquiv = extractJsonInt(cleaned, "semanticEquivalence");
        int overall = extractJsonInt(cleaned, "overall");

        Pattern reasonPat = Pattern.compile("\"reasoning\"\\s*:\\s*\"([^\"]*?)\"");
        Matcher rm = reasonPat.matcher(cleaned);
        String reasoning = rm.find() ? rm.group(1) : cleaned;

        return new NlqFullEvalResult.LlmJudgeScore(colSel, filtering, aggregation, semEquiv, overall, reasoning);
    }

    private static int extractJsonInt(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*(\\d)");
        Matcher m = p.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    // ==================== Full Pipeline Report ====================

    public static String generateFullReport(List<NlqFullEvalResult> results) {
        StringBuilder sb = new StringBuilder();

        int total = results.size();
        int errors = 0;
        int routingPreferred = 0;
        int routingAcceptable = 0;
        double totalOpCoverage = 0;
        double totalRoleScore = 0;
        int roleCount = 0;
        double totalJudgeOverall = 0;
        double totalJudgeCols = 0;
        double totalJudgeFilter = 0;
        double totalJudgeAgg = 0;
        double totalJudgeSemantic = 0;
        int judgeCount = 0;
        long totalLatency = 0;

        for (NlqFullEvalResult r : results) {
            if (r.hasError()) {
                errors++;
                continue;
            }

            totalLatency += r.latencyMs();

            if (r.routing() != null) {
                if (r.routing().preferredMatch()) routingPreferred++;
                if (r.routing().acceptableMatch()) routingAcceptable++;
            }
            if (r.queryAccuracy() != null) {
                totalOpCoverage += r.queryAccuracy().opCoverage();
            }
            if (r.propertyRoles() != null) {
                double roleScore = r.propertyRoles().overallScore();
                if (roleScore > 0) { totalRoleScore += roleScore; roleCount++; }
            }
            if (r.llmJudge() != null && r.llmJudge().overall() > 0) {
                totalJudgeOverall += r.llmJudge().overall();
                totalJudgeCols += r.llmJudge().columnSelection();
                totalJudgeFilter += r.llmJudge().filtering();
                totalJudgeAgg += r.llmJudge().aggregation();
                totalJudgeSemantic += r.llmJudge().semanticEquivalence();
                judgeCount++;
            }
        }

        int scored = total - errors;

        sb.append("\n═══════════════════════════════════════════════════════════\n");
        sb.append("  NLQ Full Pipeline Eval Report\n");
        sb.append(String.format("  Cases: %d | Errors: %d\n", total, errors));
        sb.append("═══════════════════════════════════════════════════════════\n\n");

        sb.append("  Routing:\n");
        sb.append(String.format("    Preferred Match:    %d/%d (%.1f%%)\n",
                routingPreferred, scored, 100.0 * routingPreferred / Math.max(1, scored)));
        sb.append(String.format("    Acceptable Match:   %d/%d (%.1f%%)\n",
                routingAcceptable, scored, 100.0 * routingAcceptable / Math.max(1, scored)));

        sb.append(String.format("\n  Avg Op Coverage:       %.1f%%\n", 100.0 * totalOpCoverage / Math.max(1, scored)));
        if (roleCount > 0) {
            sb.append(String.format("  Avg Property Role:     %.1f%%\n", 100.0 * totalRoleScore / roleCount));
        }

        sb.append(String.format("\n  LLM Judge (avg of %d):\n", judgeCount));
        sb.append(String.format("    Column Selection:   %.1f / 5.0\n", totalJudgeCols / Math.max(1, judgeCount)));
        sb.append(String.format("    Filtering:          %.1f / 5.0\n", totalJudgeFilter / Math.max(1, judgeCount)));
        sb.append(String.format("    Aggregation:        %.1f / 5.0\n", totalJudgeAgg / Math.max(1, judgeCount)));
        sb.append(String.format("    Semantic Equiv:     %.1f / 5.0\n", totalJudgeSemantic / Math.max(1, judgeCount)));
        sb.append(String.format("    Overall:            %.1f / 5.0\n", totalJudgeOverall / Math.max(1, judgeCount)));

        sb.append(String.format("\n  Avg Latency:           %d ms\n\n", totalLatency / Math.max(1, scored)));

        // Per-case detail
        sb.append("  Per-Case Detail:\n");
        for (NlqFullEvalResult r : results) {
            if (r.hasError()) {
                sb.append(String.format("    ⚠ %s: ERROR — %s\n", r.caseId(), r.error()));
                continue;
            }

            String routeStr;
            if (r.routing() != null && r.routing().preferredMatch()) routeStr = "✓";
            else if (r.routing() != null && r.routing().acceptableMatch()) routeStr = "~";
            else routeStr = "✗";

            String opsStr = r.queryAccuracy() != null
                    ? String.format("%.0f%%", r.queryAccuracy().opCoverage() * 100) : "?";
            String roleStr = r.propertyRoles() != null
                    ? String.format("%.0f%%", r.propertyRoles().overallScore() * 100) : "-";
            String judgeStr = r.llmJudge() != null && r.llmJudge().overall() > 0
                    ? String.format("%d(c%d/f%d/a%d/s%d)",
                        r.llmJudge().overall(),
                        r.llmJudge().columnSelection(),
                        r.llmJudge().filtering(),
                        r.llmJudge().aggregation(),
                        r.llmJudge().semanticEquivalence())
                    : "?";

            boolean good = r.llmJudge() != null && r.llmJudge().overall() >= 4;
            String icon = good ? "✅" : "❌";

            sb.append(String.format("    %s [%s] root=%s(%s) ops=%s roles=%s judge=%s\n",
                    icon, r.caseId(),
                    r.routing() != null ? r.routing().actual() : "?",
                    routeStr, opsStr, roleStr, judgeStr));

            // Show detail for failures
            if (!good) {
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
