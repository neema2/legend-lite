package org.finos.legend.engine.nlq.eval;

import java.util.List;

/**
 * A single NLQ evaluation test case, loaded from JSON.
 */
public record NlqEvalCase(
        String id,
        String question,
        String subdomain,
        String difficulty,
        ExpectedOutcome expected
) {
    public record ExpectedOutcome(
            RetrievalExpectation retrieval,
            String rootClass,
            QueryExpectation query
    ) {}

    public record RetrievalExpectation(
            List<String> mustInclude,
            List<String> mustExclude,
            int maxK
    ) {}

    public record QueryExpectation(
            String referenceQuery,
            List<String> mustContainOps,
            List<String> mustReferenceProperties
    ) {}
}
