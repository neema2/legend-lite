// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.door.ModelFromText;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.protocol.ModelReader;
import com.legend.protocol.ProtocolEmitter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Kafka connection (docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §4.6, the desk roadmap's B2): {@code type: Kafka}
 * and the {@code Kafka} specification -- a topic store's brokers, its records' format, an Avro topic's schema registry
 * -- a DECLARED lite extension, as Deephaven's is. lite's dialect reads it; legend-engine's refuses each word by name;
 * its JSON is lite's own; no query runs on it, so no dialect exists for it, said by name.
 */
class KafkaConnectionTest {

    private static String connection(String type, String specification) {
        return "###Connection\n"
                + "RelationalDatabaseConnection demo::TickerPlant\n"
                + "{\n"
                + "  store: tickerplant::Topics;\n"
                + "  type: " + type + ";\n"
                + "  specification: " + specification + ";\n"
                + "  auth: Test;\n"
                + "}\n";
    }

    private static final String SPEC = "Kafka { bootstrapServers: '127.0.0.1:19092'; format: Json; }";
    private static final String TEXT = connection("Kafka", SPEC);

    private static void refusedNaming(String text, Dialect dialect, String message) {
        ParseException e = assertThrows(ParseException.class, () -> ModelFromText.parse(text, dialect));
        assertTrue(String.valueOf(e.getMessage()).contains(message), e.getMessage());
    }

    @Test
    void litesDialectReadsTheConnection() {
        ConnectionDefinition c = (ConnectionDefinition) ModelFromText.parse(TEXT, Dialect.LEGEND_LITE).elements()
                .get(0);
        assertEquals(new ConnectionDefinition("demo::TickerPlant", "tickerplant::Topics",
                ConnectionDefinition.DatabaseType.Kafka,
                new ConnectionSpecification.Kafka("127.0.0.1:19092", "Json", null),
                new AuthenticationSpec.TestAuth()), c);
        ConnectionDefinition avro = (ConnectionDefinition) ModelFromText.parse(connection("Kafka",
                "Kafka { bootstrapServers: 'k1:9093,k2:9093'; schemaRegistry: 'https://schemas'; format: Avro; }"),
                Dialect.LEGEND_LITE).elements().get(0);
        assertEquals(new ConnectionSpecification.Kafka("k1:9093,k2:9093", "Avro", "https://schemas"),
                avro.specification());
    }

    @Test
    void theEnginesDialectRefusesEachWord() {
        refusedNaming(TEXT, Dialect.LEGEND_ENGINE, "Unknown database type 'Kafka'");
        refusedNaming(connection("Postgres", SPEC), Dialect.LEGEND_ENGINE, "unsupported datasource specification: Kafka");
    }

    @Test
    void theBrokersAndTheFormatAreRequired_andAnyOtherKeyOrFormatRefusedByName() {
        refusedNaming(connection("Kafka", "Kafka { format: Json; }"), Dialect.LEGEND_LITE, "Kafka needs bootstrapServers");
        refusedNaming(connection("Kafka", "Kafka { bootstrapServers: 'k:9092'; }"), Dialect.LEGEND_LITE,
                "Kafka needs format");
        refusedNaming(connection("Kafka", "Kafka { bootstrapServers: 'k:9092'; format: Csv; }"), Dialect.LEGEND_LITE,
                "a Kafka topic's format is Json or Avro, not Csv");
        refusedNaming(connection("Kafka", "Kafka { bootstrapServers: 'k:9092'; format: Json; topic: 'x'; }"),
                Dialect.LEGEND_LITE, "unknown Kafka key: topic");
    }

    @Test
    void itsJsonIsLitesOwn_readBackTheSame() {
        String bare = PmcdParser.parseLiteDocument(TEXT, false);
        assertTrue(bare.contains("{\"_type\":\"kafka\",\"bootstrapServers\":\"127.0.0.1:19092\",\"format\":\"Json\"}"),
                bare);
        String json = PmcdParser.parseLiteDocument(TEXT, true);
        assertEquals(json, ProtocolEmitter.emit(ModelReader.read(json)));
        var printed = com.legend.server.PureV1Api.jsonToGrammarModel(json, null);
        assertTrue(printed.json().contains("no composer rule for the Kafka datasource"), printed.json());
    }

    @Test
    void noQueryRunsOnIt() {
        var none = assertThrows(com.legend.error.NotImplementedException.class,
                () -> com.legend.database.Databases.dialect(ConnectionDefinition.DatabaseType.Kafka));
        assertTrue(none.getMessage().contains("a Kafka connection is a topic store's brokers: no query runs on it"),
                none.getMessage());
    }
}
