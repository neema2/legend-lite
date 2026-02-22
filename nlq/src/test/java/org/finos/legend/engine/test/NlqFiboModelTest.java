package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NLQ FIBO Model — Smoke Tests")
class NlqFiboModelTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws IOException {
        String pureSource;
        try (InputStream is = NlqFiboModelTest.class.getResourceAsStream("/nlq/fibo-model.pure")) {
            assertNotNull(is, "FIBO model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);
        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    @Test
    @DisplayName("Model loads 2500+ classes")
    void testClassCount() {
        int count = modelBuilder.getAllClasses().size();
        System.out.println("FIBO classes: " + count);
        assertTrue(count >= 2500, "Expected >= 2500 classes, got " + count);
    }

    @Test
    @DisplayName("Index is populated")
    void testIndexSize() {
        assertTrue(index.size() >= 2500, "Index too small: " + index.size());
    }

    @Test
    @DisplayName("Retrieval: bond security")
    void testRetrievalBond() {
        var results = index.retrieve("bond security fixed income debt", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'bond security' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.toLowerCase().contains("bond") || n.toLowerCase().contains("debt")),
                "Expected bond/debt class, got: " + names);
    }

    @Test
    @DisplayName("Retrieval: equity stock")
    void testRetrievalEquity() {
        var results = index.retrieve("equity stock share listed", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'equity stock' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.toLowerCase().contains("equit") || n.toLowerCase().contains("share") || n.toLowerCase().contains("stock")),
                "Expected equity class, got: " + names);
    }

    @Test
    @DisplayName("Retrieval: derivative option")
    void testRetrievalDerivative() {
        var results = index.retrieve("derivative option contract swap forward", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'derivative option' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.toLowerCase().contains("option") || n.toLowerCase().contains("deriv") || n.toLowerCase().contains("swap")),
                "Expected derivative class, got: " + names);
    }

    @Test
    @DisplayName("Model statistics")
    void testStats() {
        var all = modelBuilder.getAllClasses();
        int totalProps = all.values().stream().mapToInt(c -> c.properties().size()).sum();
        Set<String> domains = new HashSet<>();
        all.keySet().forEach(n -> { if (n.contains("::")) domains.add(n.substring(0, n.indexOf("::"))); });
        System.out.printf("\n  FIBO: %d classes, %d properties, %d domains%n", all.size(), totalProps, domains.size());
    }
}
