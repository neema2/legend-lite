package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NLQ BIAN Model — Smoke Tests")
class NlqBianModelTest {

    private static PureModelBuilder modelBuilder;
    private static SemanticIndex index;

    @BeforeAll
    static void setup() throws IOException {
        String pureSource;
        try (InputStream is = NlqBianModelTest.class.getResourceAsStream("/nlq/bian-model.pure")) {
            assertNotNull(is, "BIAN model resource not found");
            pureSource = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        modelBuilder = new PureModelBuilder();
        modelBuilder.addSource(pureSource);
        index = new SemanticIndex();
        index.buildIndex(modelBuilder);
    }

    @Test
    @DisplayName("Model loads 1200+ classes")
    void testClassCount() {
        int count = modelBuilder.getAllClasses().size();
        System.out.println("BIAN classes: " + count);
        assertTrue(count >= 1200, "Expected >= 1200 classes, got " + count);
    }

    @Test
    @DisplayName("Index is populated")
    void testIndexSize() {
        assertTrue(index.size() >= 1200, "Index too small: " + index.size());
    }

    @Test
    @DisplayName("Retrieval: payment transfer")
    void testRetrievalPayment() {
        var results = index.retrieve("payment transfer wire bank", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'payment transfer' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.toLowerCase().contains("payment") || n.toLowerCase().contains("transfer")),
                "Expected payment class, got: " + names);
    }

    @Test
    @DisplayName("Retrieval: loan account")
    void testRetrievalLoan() {
        var results = index.retrieve("consumer loan mortgage credit facility", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'loan' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.toLowerCase().contains("loan") || n.toLowerCase().contains("credit") || n.toLowerCase().contains("lend")),
                "Expected loan class, got: " + names);
    }

    @Test
    @DisplayName("Retrieval: customer account")
    void testRetrievalAccount() {
        var results = index.retrieve("customer bank account balance", 15, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'account' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.toLowerCase().contains("account") || n.toLowerCase().contains("balance")),
                "Expected account class, got: " + names);
    }

    @Test
    @DisplayName("Model statistics")
    void testStats() {
        var all = modelBuilder.getAllClasses();
        int totalProps = all.values().stream().mapToInt(c -> c.properties().size()).sum();
        Set<String> domains = new HashSet<>();
        all.keySet().forEach(n -> { if (n.contains("::")) domains.add(n.substring(0, n.indexOf("::"))); });
        System.out.printf("\n  BIAN: %d classes, %d properties, %d domains%n", all.size(), totalProps, domains.size());
    }
}
