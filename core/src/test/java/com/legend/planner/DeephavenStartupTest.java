// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DEEPHAVEN SERVER'S STARTUP PROGRAM, FROM THE MODEL (docs/DESK_DATA_ROADMAP_2026_10_29.md B2): each store a runtime
 * binds to a Deephaven connection, its streams' tables -- each a Kafka subscription to its topic on the brokers the
 * runtime binds the topic's store to, kept as its dataset lands (a ring of its retention, or each key's last row), its
 * days made dates on the server, its declared columns alone published under its name. A topic store bound to no Kafka
 * connection, a time retention and an Avro topic are refused by name.
 */
class DeephavenStartupTest {

    private static final String MODEL = """
            ###Relational
            Database t::Topics
            (
                Table "tp.trades.v1" (ID VARCHAR(20) PRIMARY KEY, AT TIMESTAMP, PX DOUBLE, DAY DATE)
                Table "tp.quotes.v1" (SYM VARCHAR(8) PRIMARY KEY, BID DOUBLE)
            )

            ###Lakehouse
            Ingest t::Feed Stream
            [
              TRADES( |#>{t::Topics."tp.trades.v1"}# ) pk=[ ID ] writeMode=AppendOnly retention=LastRows(500);
              QUOTES( |#>{t::Topics."tp.quotes.v1"}# ) pk=[ SYM ] writeMode=LatestByKey;
            ]

            ###Relational
            Database t::DB
            (
              include Ingest t::Feed
            )

            ###Connection
            RelationalDatabaseConnection t::Server
            {
              store: t::DB;
              type: Deephaven;
              specification: Deephaven { host: 'localhost'; port: 10000; useTls: false; };
              auth: PreSharedKey { keyVaultReference: 'dh.psk'; };
            }
            RelationalDatabaseConnection t::Brokers
            {
              store: t::Topics;
              type: Kafka;
              specification: Kafka { bootstrapServers: '127.0.0.1:19092'; format: Json; };
              auth: Test;
            }

            ###Runtime
            Runtime t::OnDeephaven
            {
              mappings: [];
              connections: [ t::DB: [ server: t::Server ], t::Topics: [ brokers: t::Brokers ] ];
            }
            """;

    private static String program(String model) {
        return DeephavenStartup.program(com.legend.Compiler.compileModel(model), "t::OnDeephaven");
    }

    @Test
    void eachStreamTableIsASubscriptionKeptAsItsDatasetLands() {
        String p = program(MODEL);
        assertTrue(p.contains("TRADES = KafkaTools.consumeToTable(legendKafka('127.0.0.1:19092'), 'tp.trades.v1',"
                + " KafkaTools.ALL_PARTITIONS, KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING, KafkaTools.Consume.IGNORE,"
                + " KafkaTools.Consume.jsonSpec([ColumnDefinition.ofString('ID'), ColumnDefinition.ofTime('AT'),"
                + " ColumnDefinition.ofDouble('PX'), ColumnDefinition.ofString('DAY')] as ColumnDefinition[]),"
                + " KafkaTools.TableType.ring(500))"
                + ".update('DAY = isNull(DAY) ? (java.time.LocalDate) null : java.time.LocalDate.parse(DAY)')"
                + ".view('ID', 'AT', 'PX', 'DAY')\n"), p);
        assertTrue(p.contains("QUOTES = KafkaTools.consumeToTable(legendKafka('127.0.0.1:19092'), 'tp.quotes.v1',"
                + " KafkaTools.ALL_PARTITIONS, KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING, KafkaTools.Consume.IGNORE,"
                + " KafkaTools.Consume.jsonSpec([ColumnDefinition.ofString('SYM'), ColumnDefinition.ofDouble('BID')]"
                + " as ColumnDefinition[]), KafkaTools.TableType.blink()).lastBy('SYM').view('SYM', 'BID')\n"), p);
        assertEquals(p, program(MODEL), "the same model, the same program");
    }

    @Test
    void whatItCannotSubscribeIsRefusedByName() {
        var unbound = assertThrows(IllegalArgumentException.class, () -> program(MODEL.replace(
                ", t::Topics: [ brokers: t::Brokers ]", "")));
        assertTrue(unbound.getMessage().contains("reads the topic tp.trades.v1 of t::Topics, which the runtime"
                + " t::OnDeephaven binds to no Kafka connection"), unbound.getMessage());
        var time = assertThrows(com.legend.sql.DialectCapability.class, () -> program(MODEL.replace(
                "retention=LastRows(500)", "retention=Last('PT8H')")));
        assertTrue(time.getMessage().contains("keeps Last('PT8H'): a retention over a time is not generated yet"),
                time.getMessage());
        var avro = assertThrows(com.legend.sql.DialectCapability.class, () -> program(MODEL.replace("format: Json;",
                "format: Avro;")));
        assertTrue(avro.getMessage().contains("is read from the Avro topic"), avro.getMessage());
    }
}
