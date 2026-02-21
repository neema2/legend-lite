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
            List<String> acceptableRootClasses,
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
            PropertyRoles properties
    ) {}

    /**
     * Role-based property expectations.
     * Properties are categorized by how they should be used in the query.
     */
    public record PropertyRoles(
            List<String> dimensions,
            List<MetricExpectation> metrics,
            List<String> filters,
            List<String> sortedBy
    ) {
        public static final PropertyRoles EMPTY = new PropertyRoles(List.of(), List.of(), List.of(), List.of());
    }

    public record MetricExpectation(
            String property,
            String function
    ) {}
}
