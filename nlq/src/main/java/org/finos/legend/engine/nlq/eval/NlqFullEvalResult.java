package org.finos.legend.engine.nlq.eval;

import java.util.List;

/**
 * Full pipeline eval result for a single case — covers retrieval, routing, query accuracy, and LLM judge.
 */
public record NlqFullEvalResult(
        String caseId,
        String question,
        String difficulty,
        NlqEvalResult.RetrievalScore retrieval,
        RoutingScore routing,
        QueryAccuracyScore queryAccuracy,
        PropertyRoleScore propertyRoles,
        LlmJudgeScore llmJudge,
        String generatedQuery,
        String referenceQuery,
        long latencyMs,
        String error
) {

    public record RoutingScore(
            String preferred,
            List<String> acceptable,
            String actual,
            boolean preferredMatch,
            boolean acceptableMatch
    ) {}

    public record QueryAccuracyScore(
            boolean parseable,
            double opCoverage,
            List<String> missingOps
    ) {}

    /**
     * Scores how well properties are used in the correct roles.
     */
    public record PropertyRoleScore(
            double dimensionCoverage,
            double metricCoverage,
            double filterCoverage,
            double sortCoverage,
            List<String> missingDimensions,
            List<String> missingMetrics,
            List<String> missingFilters,
            List<String> missingSorts
    ) {
        public double overallScore() {
            int count = 0;
            double sum = 0;
            if (!missingDimensions.isEmpty() || dimensionCoverage > 0) { sum += dimensionCoverage; count++; }
            if (!missingMetrics.isEmpty() || metricCoverage > 0) { sum += metricCoverage; count++; }
            if (!missingFilters.isEmpty() || filterCoverage > 0) { sum += filterCoverage; count++; }
            if (!missingSorts.isEmpty() || sortCoverage > 0) { sum += sortCoverage; count++; }
            return count > 0 ? sum / count : 1.0;
        }
    }

    /**
     * Structured LLM judge with multi-dimension sub-scores.
     */
    public record LlmJudgeScore(
            int columnSelection,
            int filtering,
            int aggregation,
            int semanticEquivalence,
            int overall,
            String reasoning
    ) {}

    public boolean hasError() {
        return error != null && !error.isEmpty();
    }

    public static NlqFullEvalResult error(String caseId, String question, String difficulty, String error) {
        return new NlqFullEvalResult(caseId, question, difficulty, null, null, null, null, null, null, null, 0, error);
    }
}
