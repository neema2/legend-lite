package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.eval.NlqEvalCase;
import org.finos.legend.engine.nlq.eval.NlqEvalRunner;
import org.finos.legend.pure.dsl.PureParser;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that every referenceQuery in eval case files is syntactically valid Pure.
 * Runs on every build — no LLM required.
 */
@DisplayName("Reference Query Parse Validation")
class NlqReferenceQueryParseTest {

    private static List<NlqEvalCase> salesTradingCases;
    private static List<NlqEvalCase> cdmCases;

    @BeforeAll
    static void loadCases() throws IOException {
        salesTradingCases = NlqEvalRunner.loadCases("/nlq/nlq-eval-cases.json");
        cdmCases = NlqEvalRunner.loadCases("/nlq/cdm-eval-cases.json");
    }

    record QueryCase(String id, String referenceQuery) {
        @Override
        public String toString() {
            return id;
        }
    }

    static Stream<QueryCase> salesTradingQueries() {
        return salesTradingCases.stream()
                .filter(c -> c.expected().query() != null && c.expected().query().referenceQuery() != null)
                .map(c -> new QueryCase(c.id(), c.expected().query().referenceQuery()));
    }

    static Stream<QueryCase> cdmQueries() {
        return cdmCases.stream()
                .filter(c -> c.expected().query() != null && c.expected().query().referenceQuery() != null)
                .map(c -> new QueryCase(c.id(), c.expected().query().referenceQuery()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("salesTradingQueries")
    @DisplayName("Sales-Trading reference queries parse")
    void testSalesTradingReferenceQueryParses(QueryCase qc) {
        assertDoesNotThrow(
                () -> PureParser.parse(qc.referenceQuery()),
                qc.id() + " referenceQuery failed to parse: " + qc.referenceQuery()
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cdmQueries")
    @DisplayName("CDM reference queries parse")
    void testCdmReferenceQueryParses(QueryCase qc) {
        assertDoesNotThrow(
                () -> PureParser.parse(qc.referenceQuery()),
                qc.id() + " referenceQuery failed to parse: " + qc.referenceQuery()
        );
    }

    @Test
    @DisplayName("All sales-trading cases have reference queries")
    void testAllSalesTradingCasesHaveQueries() {
        for (NlqEvalCase c : salesTradingCases) {
            assertNotNull(c.expected().query(), c.id() + " missing query expectation");
            assertNotNull(c.expected().query().referenceQuery(), c.id() + " missing referenceQuery");
            assertFalse(c.expected().query().referenceQuery().isBlank(), c.id() + " has blank referenceQuery");
        }
    }

    @Test
    @DisplayName("All CDM cases have reference queries")
    void testAllCdmCasesHaveQueries() {
        for (NlqEvalCase c : cdmCases) {
            assertNotNull(c.expected().query(), c.id() + " missing query expectation");
            assertNotNull(c.expected().query().referenceQuery(), c.id() + " missing referenceQuery");
            assertFalse(c.expected().query().referenceQuery().isBlank(), c.id() + " has blank referenceQuery");
        }
    }
}
