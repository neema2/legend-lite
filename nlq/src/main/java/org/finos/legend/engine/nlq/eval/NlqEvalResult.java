package org.finos.legend.engine.nlq.eval;

import java.util.List;
import java.util.Set;

/**
 * Scored result for a single eval case at the retrieval stage.
 */
public record NlqEvalResult(
        String caseId,
        String question,
        RetrievalScore retrieval,
        long latencyMs,
        String error
) {

    public record RetrievalScore(
            double recall,
            double exclusionRate,
            boolean allMustInclude,
            boolean allMustExclude,
            List<String> retrievedClasses,
            Set<String> missingClasses,
            Set<String> unexpectedClasses
    ) {
        /**
         * Combined retrieval score: 60% recall + 40% exclusion.
         */
        public double score() {
            return 0.6 * recall + 0.4 * exclusionRate;
        }

        public boolean passed() {
            return allMustInclude && allMustExclude;
        }
    }

    public boolean hasError() {
        return error != null && !error.isEmpty();
    }
}
