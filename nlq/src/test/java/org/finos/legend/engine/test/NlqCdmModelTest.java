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
 * Smoke tests for the ISDA CDM Pure model.
 * Validates the auto-generated model loads, indexes, and can be queried.
 */
@DisplayName("NLQ ISDA CDM Model — Smoke Tests")
class NlqCdmModelTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws IOException {
        String pureSource;
        try (InputStream is = NlqCdmModelTest.class.getResourceAsStream("/nlq/cdm-model.pure")) {
            assertNotNull(is, "CDM model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    @Test
    @DisplayName("Model loads 700+ classes")
    void testClassCount() {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        System.out.println("CDM classes loaded: " + allClasses.size());
        assertTrue(allClasses.size() >= 700,
                "Expected at least 700 classes, got " + allClasses.size());
    }

    @Test
    @DisplayName("Semantic index is populated")
    void testIndexSize() {
        System.out.println("CDM index entries: " + index.size());
        assertTrue(index.size() >= 700,
                "Expected at least 700 indexed entries, got " + index.size());
    }

    @Test
    @DisplayName("Key product classes exist")
    void testProductClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("template::EconomicTerms"), "EconomicTerms missing");
        assertNotNull(modelBuilder.getAllClasses().get("template::TransferableProduct"), "TransferableProduct missing");
        assertNotNull(modelBuilder.getAllClasses().get("product::InterestRatePayout"), "InterestRatePayout missing");
    }

    @Test
    @DisplayName("Key event classes exist")
    void testEventClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("event::BusinessEvent"), "BusinessEvent missing");
        assertNotNull(modelBuilder.getAllClasses().get("event::TradeState"), "TradeState missing");
        assertNotNull(modelBuilder.getAllClasses().get("event::Trade"), "Trade missing");
    }

    @Test
    @DisplayName("Key party/asset classes exist")
    void testRefDataClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("party::Party"), "Party missing");
        assertNotNull(modelBuilder.getAllClasses().get("asset::AssetIdentifier"), "AssetIdentifier missing");
    }

    @Test
    @DisplayName("Retrieval: interest rate swap query")
    void testRetrievalSwap() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("interest rate swap payout fixed floating", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'interest rate swap' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n ->
                        n.contains("InterestRate") || n.contains("Payout") || n.contains("Swap")),
                "Expected IR/swap class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: trade lifecycle event query")
    void testRetrievalEvent() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("trade execution business event lifecycle", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'trade event' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n ->
                        n.contains("Trade") || n.contains("Event") || n.contains("Execution")),
                "Expected trade/event class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: collateral query")
    void testRetrievalCollateral() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("collateral eligibility criteria", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'collateral' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n ->
                        n.contains("Collateral") || n.contains("Eligib") || n.contains("collateral")),
                "Expected collateral class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: settlement query")
    void testRetrievalSettlement() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("cash settlement physical delivery instructions", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'settlement' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n ->
                        n.contains("Settlement") || n.contains("Delivery") || n.contains("Transfer")),
                "Expected settlement class, got: " + classNames);
    }

    @Test
    @DisplayName("Schema extraction works for CDM model")
    void testSchemaExtraction() {
        Set<String> classNames = Set.of(
                "event::Trade",
                "event::BusinessEvent",
                "template::EconomicTerms"
        );
        String schema = ModelSchemaExtractor.extractSchema(classNames, modelBuilder);
        assertNotNull(schema);
        assertFalse(schema.isEmpty());
        assertTrue(schema.contains("Trade"), "Schema should contain Trade");
        assertTrue(schema.contains("EconomicTerms"), "Schema should contain EconomicTerms");
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

        Set<String> domains = new HashSet<>();
        for (String name : allClasses.keySet()) {
            if (name.contains("::")) {
                domains.add(name.substring(0, name.indexOf("::")));
            }
        }

        System.out.println("\n═══════════════════════════════════════════════════");
        System.out.println("  ISDA CDM Pure Model Statistics");
        System.out.println("═══════════════════════════════════════════════════");
        System.out.printf("  Classes:      %d%n", allClasses.size());
        System.out.printf("  Properties:   %d%n", totalProps);
        System.out.printf("  Domains:      %d (%s)%n", domains.size(), domains);
        System.out.printf("  Index entries: %d%n", index.size());
        System.out.println("═══════════════════════════════════════════════════");
    }
}
