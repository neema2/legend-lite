package org.finos.legend.engine.test;

import org.finos.legend.engine.nlq.SemanticIndex;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NLQ ACTUS + CRIF Models — Smoke Tests")
class NlqActusCrifModelTest {

    private static PureModelBuilder actusModel;
    private static SemanticIndex actusIndex;
    private static PureModelBuilder crifModel;
    private static SemanticIndex crifIndex;

    @BeforeAll
    static void setup() throws IOException {
        // ACTUS
        try (InputStream is = NlqActusCrifModelTest.class.getResourceAsStream("/nlq/actus-model.pure")) {
            assertNotNull(is, "ACTUS model not found");
            String src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            actusModel = new PureModelBuilder();
            actusModel.addSource(src);
            actusIndex = new SemanticIndex();
            actusIndex.buildIndex(actusModel);
        }
        // CRIF
        try (InputStream is = NlqActusCrifModelTest.class.getResourceAsStream("/nlq/crif-model.pure")) {
            assertNotNull(is, "CRIF model not found");
            String src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            crifModel = new PureModelBuilder();
            crifModel.addSource(src);
            crifIndex = new SemanticIndex();
            crifIndex.buildIndex(crifModel);
        }
    }

    // ─── ACTUS ───

    @Test
    @DisplayName("ACTUS: loads 30+ contract types")
    void testActusClassCount() {
        int count = actusModel.getAllClasses().size();
        System.out.println("ACTUS classes: " + count);
        assertTrue(count >= 30, "Expected >= 30, got " + count);
    }

    @Test
    @DisplayName("ACTUS: retrieval — annuity bond")
    void testActusRetrieval() {
        var results = actusIndex.retrieve("annuity bond fixed income principal repayment", 10, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'annuity bond' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.contains("Annuity") || n.contains("Bond") || n.contains("Principal")),
                "Expected annuity/bond class, got: " + names);
    }

    @Test
    @DisplayName("ACTUS: retrieval — option swap")
    void testActusOptionSwap() {
        var results = actusIndex.retrieve("option cap floor swap interest rate", 10, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'option swap' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.contains("Cap") || n.contains("Option") || n.contains("Swap")),
                "Expected option/swap class, got: " + names);
    }

    // ─── CRIF ───

    @Test
    @DisplayName("CRIF: loads 10+ classes")
    void testCrifClassCount() {
        int count = crifModel.getAllClasses().size();
        System.out.println("CRIF classes: " + count);
        assertTrue(count >= 10, "Expected >= 10, got " + count);
    }

    @Test
    @DisplayName("CRIF: retrieval — SIMM margin")
    void testCrifRetrieval() {
        var results = crifIndex.retrieve("SIMM initial margin sensitivity risk", 10, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'SIMM margin' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.contains("SIMM") || n.contains("CRIF") || n.contains("Sensitivity") || n.contains("Margin")),
                "Expected SIMM/CRIF class, got: " + names);
    }

    @Test
    @DisplayName("CRIF: retrieval — FRTB capital")
    void testCrifFRTB() {
        var results = crifIndex.retrieve("FRTB standardized approach capital requirement", 10, null);
        var names = results.stream().map(SemanticIndex.RetrievalResult::qualifiedName).toList();
        System.out.println("  'FRTB capital' → " + names);
        assertTrue(names.stream().anyMatch(n -> n.contains("FRTB") || n.contains("Capital") || n.contains("Risk")),
                "Expected FRTB class, got: " + names);
    }

    @Test
    @DisplayName("Combined statistics")
    void testStats() {
        var actusAll = actusModel.getAllClasses();
        int actusProps = actusAll.values().stream().mapToInt(c -> c.properties().size()).sum();
        var crifAll = crifModel.getAllClasses();
        int crifProps = crifAll.values().stream().mapToInt(c -> c.properties().size()).sum();
        System.out.printf("\n  ACTUS: %d classes, %d properties%n", actusAll.size(), actusProps);
        System.out.printf("  CRIF:  %d classes, %d properties%n", crifAll.size(), crifProps);
    }
}
