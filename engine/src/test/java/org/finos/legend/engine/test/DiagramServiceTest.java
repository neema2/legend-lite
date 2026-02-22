package org.finos.legend.engine.test;

import org.finos.legend.engine.server.DiagramService;
import org.finos.legend.engine.server.DiagramService.*;
import org.finos.legend.engine.server.LegendHttpServer;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DiagramService — extracts classes, associations, and
 * generalisations from Pure source via PureModelBuilder.
 */
class DiagramServiceTest {

    private final DiagramService service = new DiagramService();

    // ── Minimal inline model ──

    @Test
    void extractMinimalModel() {
        String pure = """
                Class Person { firstName: String[1]; age: Integer[1]; }
                Class Address { city: String[1]; }
                Association Person_Address { person: Person[1]; addresses: Address[*]; }
                """;

        DiagramData data = service.extract(pure);

        // Classes
        assertEquals(2, data.classes().size());
        Set<String> classNames = data.classes().stream().map(ClassInfo::name).collect(Collectors.toSet());
        assertTrue(classNames.contains("Person"));
        assertTrue(classNames.contains("Address"));

        // Properties on Person
        ClassInfo person = data.classes().stream()
                .filter(c -> c.name().equals("Person")).findFirst().orElseThrow();
        assertEquals(2, person.properties().size());
        assertEquals("firstName", person.properties().get(0).name());
        assertEquals("String", person.properties().get(0).type());

        // Association
        assertEquals(1, data.associations().size());
        AssociationInfo assoc = data.associations().get(0);
        assertEquals("Person_Address", assoc.name());
        // One end points to Person, the other to Address
        Set<String> endpoints = Set.of(assoc.source(), assoc.target());
        assertTrue(endpoints.contains("Person"));
        assertTrue(endpoints.contains("Address"));
    }

    @Test
    void extractInheritance() {
        String pure = """
                Class Animal { name: String[1]; }
                Class Dog extends Animal { breed: String[1]; }
                """;

        DiagramData data = service.extract(pure);
        assertEquals(2, data.classes().size());
        assertEquals(1, data.generalisations().size());

        GeneralisationInfo gen = data.generalisations().get(0);
        assertEquals("Dog", gen.child());
        assertEquals("Animal", gen.parent());
    }

    @Test
    void extractWithAnnotations() {
        String pure = """
                Profile nlq::NlqProfile
                {
                    stereotypes: [dimension, metric];
                    tags: [description, businessDomain];
                }

                Class <<nlq::NlqProfile.dimension>>
                  {nlq::NlqProfile.description = 'A financial trade',
                   nlq::NlqProfile.businessDomain = 'Trading'}
                trading::Trade
                {
                    tradeId: String[1];
                    notional: Float[1];
                }
                """;

        DiagramData data = service.extract(pure);
        assertEquals(1, data.classes().size());

        ClassInfo trade = data.classes().get(0);
        assertEquals("Trade", trade.name());
        assertEquals("trading", trade.packagePath());
        assertEquals("trading::Trade", trade.id());
        assertEquals("dimension", trade.stereotype());
        assertEquals("A financial trade", trade.description());
        assertEquals("Trading", trade.businessDomain());
        assertEquals(2, trade.properties().size());
    }

    // ── Sales-trading model (the real thing) ──

    @Test
    void extractSalesTradingModel() throws IOException {
        Path modelPath = Path.of("../nlq/src/test/resources/nlq/sales-trading-model.pure");
        if (!Files.exists(modelPath)) {
            // Skip if model file not available (CI without nlq module)
            return;
        }
        String pure = Files.readString(modelPath);
        DiagramData data = service.extract(pure);

        // Should have ~48 classes and ~55 associations
        assertTrue(data.classes().size() >= 40,
                "Expected ≥40 classes, got " + data.classes().size());
        assertTrue(data.associations().size() >= 40,
                "Expected ≥40 associations, got " + data.associations().size());

        // Verify key classes exist
        Set<String> classIds = data.classes().stream()
                .map(ClassInfo::id).collect(Collectors.toSet());
        assertTrue(classIds.contains("trading::Trade"), "Missing trading::Trade");
        assertTrue(classIds.contains("products::Instrument"), "Missing products::Instrument");
        assertTrue(classIds.contains("pnl::DailyPnL"), "Missing pnl::DailyPnL");
        assertTrue(classIds.contains("risk::VaRResult"), "Missing risk::VaRResult");

        // Verify key associations exist
        Set<String> assocNames = data.associations().stream()
                .map(AssociationInfo::name).collect(Collectors.toSet());
        assertTrue(assocNames.contains("Trade_Instrument"), "Missing Trade_Instrument assoc");
        assertTrue(assocNames.contains("Trade_Counterparty"), "Missing Trade_Counterparty assoc");

        // Verify some classes have business domain set
        ClassInfo trade = data.classes().stream()
                .filter(c -> c.id().equals("trading::Trade")).findFirst().orElseThrow();
        assertFalse(trade.businessDomain().isEmpty(), "Trade should have businessDomain");
        assertTrue(trade.properties().size() >= 5,
                "Trade should have ≥5 properties, got " + trade.properties().size());

        // Verify inheritance
        Set<String> childClasses = data.generalisations().stream()
                .map(GeneralisationInfo::child).collect(Collectors.toSet());
        assertTrue(childClasses.contains("products::Equity") || childClasses.contains("products::Bond"),
                "Expected some product subclasses");

        System.out.println("Sales-Trading diagram: " + data.classes().size() + " classes, "
                + data.associations().size() + " associations, "
                + data.generalisations().size() + " generalisations");
    }

    // ── JSON serialisation ──

    @Test
    void toJsonRoundTrip() {
        String pure = """
                Class Foo { x: String[1]; }
                Class Bar { y: Integer[0..1]; }
                Association Foo_Bar { foo: Foo[1]; bar: Bar[*]; }
                """;

        DiagramData data = service.extract(pure);
        String json = service.toJson(data);

        // Verify it's valid JSON structure
        assertTrue(json.startsWith("{\"classes\":["));
        assertTrue(json.contains("\"associations\":["));
        assertTrue(json.contains("\"generalisations\":["));
        assertTrue(json.endsWith("]}"));

        // Verify key content
        assertTrue(json.contains("\"name\":\"Foo\""), "JSON should contain Foo");
        assertTrue(json.contains("\"name\":\"Bar\""), "JSON should contain Bar");
        assertTrue(json.contains("\"name\":\"Foo_Bar\""), "JSON should contain Foo_Bar assoc");
    }

    // ── HTTP integration test ──

    @Test
    void httpEndpointReturnsDiagramJson() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0); // port 0 = random available
        server.start();
        int port = server.getPort();

        try {
            String pureCode = "Class Foo { x: String[1]; } Class Bar { y: Integer[1]; } "
                    + "Association Foo_Bar { foo: Foo[1]; bar: Bar[*]; }";

            // Build JSON request body
            String requestJson = "{\"code\":\"" + pureCode.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";

            HttpURLConnection conn = (HttpURLConnection) URI.create(
                    "http://localhost:" + port + "/engine/diagram").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write(requestJson.getBytes(StandardCharsets.UTF_8));

            assertEquals(200, conn.getResponseCode(), "Expected 200 OK");

            String body;
            try (var is = conn.getInputStream()) {
                body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            System.out.println("Diagram HTTP response: " + body.substring(0, Math.min(200, body.length())));

            assertTrue(body.startsWith("{\"classes\":"), "Response should start with {\"classes\":");
            assertTrue(body.contains("\"name\":\"Foo\""), "Should contain Foo");
            assertTrue(body.contains("\"name\":\"Bar\""), "Should contain Bar");
            assertTrue(body.contains("\"associations\":"), "Should contain associations");
            assertTrue(body.contains("\"Foo_Bar\""), "Should contain Foo_Bar");
        } finally {
            server.stop();
        }
    }

    @Test
    void httpEndpointReturnsErrorForMissingCode() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        int port = server.getPort();

        try {
            HttpURLConnection conn = (HttpURLConnection) URI.create(
                    "http://localhost:" + port + "/engine/diagram").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write("{}".getBytes(StandardCharsets.UTF_8));

            assertEquals(400, conn.getResponseCode(), "Should return 400 for missing code");
        } finally {
            server.stop();
        }
    }

    @Test
    void toJsonEscapesSpecialChars() {
        String pure = """
                Profile nlq::NlqProfile
                {
                    stereotypes: [dimension];
                    tags: [description];
                }
                Class <<nlq::NlqProfile.dimension>>
                  {nlq::NlqProfile.description = 'A description with "quotes" inside'}
                test::Quoted
                {
                    name: String[1];
                }
                """;

        DiagramData data = service.extract(pure);
        String json = service.toJson(data);

        // Should escape double quotes in description for valid JSON
        assertTrue(json.contains("\\\"quotes\\\""), "Should escape quotes in description");
    }
}
