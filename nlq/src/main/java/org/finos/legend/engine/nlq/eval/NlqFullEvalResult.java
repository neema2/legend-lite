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
        LlmJudgeScore llmJudge,
        String generatedQuery,
        String referenceQuery,
        long latencyMs,
        String error
) {

    public record RoutingScore(
            String expected,
            String actual,
            boolean correct
    ) {}

    public record QueryAccuracyScore(
            boolean parseable,
            boolean correctRoot,
            double opCoverage,
            double propertyCoverage,
            List<String> missingOps,
            List<String> missingProperties
    ) {
        public double score() {
            double rootScore = correctRoot ? 1.0 : 0.0;
            return 0.4 * rootScore + 0.3 * opCoverage + 0.3 * propertyCoverage;
        }
    }

    public record LlmJudgeScore(
            int score,
            String reasoning
    ) {}

    public boolean hasError() {
        return error != null && !error.isEmpty();
    }

    public static NlqFullEvalResult error(String caseId, String question, String difficulty, String error) {
        return new NlqFullEvalResult(caseId, question, difficulty, null, null, null, null, null, null, 0, error);
    }
}
