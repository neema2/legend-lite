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
 * Smoke tests for the GS-Quant Pure model.
 * Validates the auto-generated model loads, indexes, and can be queried.
 */
@DisplayName("NLQ GS-Quant Model — Smoke Tests")
class NlqGsQuantModelTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws IOException {
        String pureSource;
        try (InputStream is = NlqGsQuantModelTest.class.getResourceAsStream("/nlq/gsquant-model.pure")) {
            assertNotNull(is, "GS-Quant model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);

        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    @Test
    @DisplayName("Model loads 600+ classes")
    void testClassCount() {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        System.out.println("GS-Quant classes loaded: " + allClasses.size());
        assertTrue(allClasses.size() >= 600,
                "Expected at least 600 classes, got " + allClasses.size());
    }

    @Test
    @DisplayName("Semantic index is populated")
    void testIndexSize() {
        System.out.println("GS-Quant index entries: " + index.size());
        assertTrue(index.size() >= 600,
                "Expected at least 600 indexed entries, got " + index.size());
    }

    @Test
    @DisplayName("Key instrument classes exist")
    void testInstrumentClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("rates::IRSwap"), "IRSwap missing");
        assertNotNull(modelBuilder.getAllClasses().get("rates::IRSwaption"), "IRSwaption missing");
        assertNotNull(modelBuilder.getAllClasses().get("equity::EqOption"), "EqOption missing");
        assertNotNull(modelBuilder.getAllClasses().get("fx::FXOption"), "FXOption missing");
        assertNotNull(modelBuilder.getAllClasses().get("rates::Bond"), "Bond missing");
    }

    @Test
    @DisplayName("Key domain classes exist")
    void testDomainClasses() {
        assertNotNull(modelBuilder.getAllClasses().get("portfolio::Portfolio"), "Portfolio missing");
        assertNotNull(modelBuilder.getAllClasses().get("common::RiskMeasure"), "RiskMeasure missing");
        assertNotNull(modelBuilder.getAllClasses().get("secmaster::SecMasterRecord"), "SecMaster class missing");
    }

    @Test
    @DisplayName("Retrieval: interest rate swap query")
    void testRetrievalSwap() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("interest rate swap fixed vs floating", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'interest rate swap' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("IRSwap") || n.contains("Swap") || n.contains("rates")),
                "Expected swap-related class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: equity option query")
    void testRetrievalOption() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("equity call option strike price expiry", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'equity option' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("Eq") || n.contains("Option")),
                "Expected equity/option class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: portfolio query")
    void testRetrievalPortfolio() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("portfolio positions and holdings", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'portfolio positions' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("Portfolio") || n.contains("Position")),
                "Expected portfolio/position class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: risk measure query")
    void testRetrievalRisk() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("risk measure delta gamma vega", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'risk measures' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("Risk") || n.contains("risk")),
                "Expected risk class, got: " + classNames);
    }

    @Test
    @DisplayName("Retrieval: FX forward query")
    void testRetrievalFX() {
        List<SemanticIndex.RetrievalResult> results = index.retrieve("FX forward currency pair", 15, null);
        List<String> classNames = results.stream()
                .map(SemanticIndex.RetrievalResult::qualifiedName)
                .toList();
        System.out.println("  'FX forward' → " + classNames);
        assertTrue(classNames.stream().anyMatch(n -> n.contains("FX") || n.contains("fx") || n.contains("Currency")),
                "Expected FX class, got: " + classNames);
    }

    @Test
    @DisplayName("Schema extraction works for gs-quant model")
    void testSchemaExtraction() {
        Set<String> classNames = Set.of(
                "rates::IRSwap",
                "equity::EqOption",
                "portfolio::Portfolio"
        );
        String schema = ModelSchemaExtractor.extractSchema(classNames, modelBuilder);
        assertNotNull(schema);
        assertFalse(schema.isEmpty());
        assertTrue(schema.contains("IRSwap"), "Schema should contain IRSwap");
        assertTrue(schema.contains("EqOption"), "Schema should contain EqOption");
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
        System.out.println("  GS-Quant Pure Model Statistics");
        System.out.println("═══════════════════════════════════════════════════");
        System.out.printf("  Classes:      %d%n", allClasses.size());
        System.out.printf("  Properties:   %d%n", totalProps);
        System.out.printf("  Domains:      %d (%s)%n", domains.size(), domains);
        System.out.printf("  Index entries: %d%n", index.size());
        System.out.println("═══════════════════════════════════════════════════");
    }
}
