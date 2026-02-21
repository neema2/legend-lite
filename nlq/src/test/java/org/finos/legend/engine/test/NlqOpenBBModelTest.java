package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.engine.nlq.ModelSchemaExtractor;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.m3.PureClass;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke tests for the OpenBB Pure model.
 * Validates the auto-generated model loads, indexes, and can be queried.
 */
@DisplayName("NLQ OpenBB Model — Smoke Tests")
class NlqOpenBBModelTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws IOException {
        String pureSource;
        try (InputStream is = NlqOpenBBModelTest.class.getResourceAsStream("/nlq/openbb-model.pure")) {
            assertNotNull(is, "OpenBB model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    @Test
    @DisplayName("Model loads 170+ classes")
    void testClassCount() {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        System.out.println("OpenBB classes loaded: " + allClasses.size());
        assertTrue(allClasses.size() >= 170,
                "Expected at least 170 classes, got " + allClasses.size());
    }

    @Test
    @DisplayName("Semantic index is populated")
    void testIndexSize() {
        System.out.println("OpenBB index entries: " + index.size());
        assertTrue(index.size() >= 170,
                "Expected at least 170 indexed entries, got " + index.size());
    }

    @Test
    @DisplayName("Key equity classes exist")
    void testEquityClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("equity::EquityHistorical"));
        assertNotNull(modelBuilder.getAllClasses().get("equity::EquityQuote"));
        assertNotNull(modelBuilder.getAllClasses().get("equity::EquityInfo"));
    }

    @Test
    @DisplayName("Key fundamentals classes exist")
    void testFundamentalsClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("fundamentals::BalanceSheet"));
        assertNotNull(modelBuilder.getAllClasses().get("fundamentals::IncomeStatement"));
        assertNotNull(modelBuilder.getAllClasses().get("fundamentals::CashFlow"));
        assertNotNull(modelBuilder.getAllClasses().get("fundamentals::KeyMetrics"));
    }

    @Test
    @DisplayName("Cross-domain classes exist")
    void testCrossDomainClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("refdata::Security"));
        assertNotNull(modelBuilder.getAllClasses().get("derivatives::OptionsChains"));
        assertNotNull(modelBuilder.getAllClasses().get("macro::ConsumerPriceIndex"));
        assertNotNull(modelBuilder.getAllClasses().get("fixedincome::TreasuryRates"));
    }

    @Test
    @DisplayName("Retrieval: stock price query returns relevant classes")
    void testRetrievalEquity() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("What is AAPL's historical stock price?", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'historical stock price' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n ->
                        n.contains("Equity") || n.contains("Historical") || n.contains("Price") || n.contains("Quote")),
                "Expected price-related class in results, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: balance sheet query routes to fundamentals")
    void testRetrievalFundamentals() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("Show me Apple's balance sheet", 10, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'balance sheet' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("BalanceSheet")),
                "Expected BalanceSheet in results");
    }

    @Test
    @DisplayName("Retrieval: options query routes to derivatives")
    void testRetrievalOptions() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("Show me the options chain for TSLA", 10, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'options chain' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("Options")),
                "Expected Options class in results");
    }

    @Test
    @DisplayName("Retrieval: GDP query routes to macro")
    void testRetrievalMacro() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("What is the current GDP growth rate?", 10, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'GDP growth' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("Gdp")),
                "Expected GDP class in results");
    }

    @Test
    @DisplayName("Retrieval: treasury yield query routes to fixed income")
    void testRetrievalFixedIncome() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("What are current treasury rates?", 10, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'treasury rates' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("Treasury") || n.contains("Yield")),
                "Expected Treasury/Yield class in results");
    }

    @Test
    @DisplayName("Schema extraction works for OpenBB model")
    void testSchemaExtraction() {
        Set<String> classNames = Set.of(
                "equity::EquityHistorical",
                "equity::EquityQuote",
                "fundamentals::BalanceSheet"
        );
        String schema = ModelSchemaExtractor.extractSchema(classNames, modelBuilder);
        assertNotNull(schema);
        assertFalse(schema.isEmpty());
        assertTrue(schema.contains("EquityHistorical"), "Schema should contain EquityHistorical");
        assertTrue(schema.contains("BalanceSheet"), "Schema should contain BalanceSheet");
        System.out.println("Schema length: " + schema.length() + " chars");
        System.out.println("Schema preview:\n" + schema.substring(0, Math.min(500, schema.length())));
    }

    @Test
    @DisplayName("Model statistics summary")
    void testModelStats() {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        int totalProps = allClasses.values().stream()
                .mapToInt(c -> c.properties().size())
                .sum();

        // Count domains
        Set<String> domains = new HashSet<>();
        for (String name : allClasses.keySet()) {
            if (name.contains("::")) {
                domains.add(name.substring(0, name.indexOf("::")));
            }
        }

        System.out.println("\n═══════════════════════════════════════════════════");
        System.out.println("  OpenBB Pure Model Statistics");
        System.out.println("═══════════════════════════════════════════════════");
        System.out.printf("  Classes:      %d%n", allClasses.size());
        System.out.printf("  Properties:   %d%n", totalProps);
        System.out.printf("  Domains:      %d (%s)%n", domains.size(), domains);
        System.out.printf("  Index entries: %d%n", index.size());
        System.out.println("═══════════════════════════════════════════════════");
    }
}
