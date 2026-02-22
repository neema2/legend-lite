package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NLQ FIX Orchestra Model — Smoke Tests")
class NlqFixModelTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws IOException {
        String pureSource;
        try (InputStream is = NlqFixModelTest.class.getResourceAsStream("/nlq/fix-model.pure")) {
            assertNotNull(is, "FIX model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);
        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    @Test
    @DisplayName("Model loads 250+ classes")
    void testClassCount() {
        int count = modelBuilder.getAllClasses().size();
        System.out.println("FIX classes: " + count);
        assertTrue(count >= 250, "Expected >= 250 classes, got " + count);
    }

    @Test
    @DisplayName("Index is populated")
    void testIndexSize() {
        assertTrue(index.size() >= 250, "Index too small: " + index.size());
    }

    @Test
    @DisplayName("Retrieval: execution report")
    void testRetrievalExecution() {
        var results = index.retrieve("execution report order fill trade", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'execution report' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.contains("Execution") || n.contains("Order") || n.contains("Trade")),
                "Expected execution class, got: " + names);
    }

    @Test
    @DisplayName("Retrieval: market data")
    void testRetrievalMarketData() {
        var results = index.retrieve("market data snapshot price quote", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'market data' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.contains("Market") || n.contains("Quote")),
                "Expected market data class, got: " + names);
    }

    @Test
    @DisplayName("Model statistics")
    void testStats() {
        var all = modelBuilder.getAllClasses();
        int totalProps = all.values().stream().mapToInt(c -> c.properties().size()).sum();
        Set<String> domains = new HashSet<>();
        all.keySet().forEach(n -> { if (n.contains("::")) domains.add(n.substring(0, n.indexOf("::"))); });
        System.out.printf("\n  FIX: %d classes, %d properties, %d domains%n", all.size(), totalProps, domains.size());
    }
}
