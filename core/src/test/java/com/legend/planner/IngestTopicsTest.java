// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TOPICS A DATABASE'S STREAMS READ (docs/DESK_DATA_ROADMAP_2026_10_29.md B1): each Stream dataset the database
 * reaches through its includes is a topic named as the topic store's table it reads, or the ingest's own for columns
 * written out; its columns the landing table's with Deephaven's types; keyed when it lands latest by key, its retention
 * kept when appended. A pull is no topic; a name Kafka cannot take is refused by name.
 */
class IngestTopicsTest {

    private static final String MODEL = """
            ###Relational
            Database t::Topics
            (
                Table "tp.trades.v1" (ID VARCHAR(20) PRIMARY KEY, AT TIMESTAMP, PX DOUBLE, N INTEGER, ODD BIT, DAY DATE)
                Table "tp.quotes.v1" (SYM VARCHAR(8) PRIMARY KEY, BID DOUBLE)
            )
            Database t::Master
            (
                Table NAMES_V (SYM VARCHAR(8) PRIMARY KEY, NAME VARCHAR(40))
            )

            ###Lakehouse
            Ingest t::Feed Stream
            [
              TRADES( |#>{t::Topics."tp.trades.v1"}# ) pk=[ ID ] writeMode=AppendOnly retention=LastRows(500);
              QUOTES( |#>{t::Topics."tp.quotes.v1"}# ) pk=[ SYM ] writeMode=LatestByKey;
            ]
            Ingest t::Status Stream<Json>
            [
              status( item: Varchar(16)[1], state: Varchar(16)[1] ) pk=[ item ] writeMode=LatestByKey;
            ]
            Ingest t::Names Pull<Daily('06:00', 'America/New_York')> writeMode=LatestByKey
            [
              NAMES( |#>{t::Master.NAMES_V}# ) pk=[ SYM ];
            ]

            ###Relational
            Database t::DB
            (
              include Ingest t::Feed
              include Ingest t::Status
              include Ingest t::Names
            )
            """;

    @Test
    void eachStreamDatasetIsATopicAndAPullIsNone() {
        List<IngestTopics.Topic> topics = IngestTopics.of(com.legend.Compiler.compileModel(MODEL), "t::DB");
        assertEquals(List.of("tp.trades.v1", "tp.quotes.v1", "t.Status.status"),
                topics.stream().map(IngestTopics.Topic::topic).toList());
        IngestTopics.Topic trades = topics.get(0);
        assertEquals("TRADES", trades.table());
        assertEquals(List.of(new IngestTopics.Column("ID", "java.lang.String"),
                new IngestTopics.Column("AT", "java.time.Instant"), new IngestTopics.Column("PX", "double"),
                new IngestTopics.Column("N", "int"), new IngestTopics.Column("ODD", "java.lang.Boolean"),
                new IngestTopics.Column("DAY", "java.time.LocalDate")), trades.columns());
        assertEquals(List.of(), trades.key(), "appended: no key a topic keeps the latest of");
        assertEquals(500L, trades.keepRows());
        IngestTopics.Topic quotes = topics.get(1);
        assertEquals(List.of("SYM"), quotes.key(), "latest by key: a compacted topic, by the dataset's key");
        assertEquals(null, quotes.keepRows());
        assertEquals(List.of("item"), topics.get(2).key());
        assertEquals(List.of("item", "state"), topics.get(2).columns().stream().map(IngestTopics.Column::name).toList());
    }

    @Test
    void aTopicKafkaCannotNameIsRefusedByName() {
        var refused = assertThrows(IllegalArgumentException.class, () -> IngestTopics.of(com.legend.Compiler
                .compileModel(MODEL.replace("\"tp.quotes.v1\"", "\"tp quotes\"")), "t::DB"));
        assertTrue(refused.getMessage().contains("reads the topic 'tp quotes', which is no Kafka topic's name"),
                refused.getMessage());
        var none = assertThrows(IllegalArgumentException.class,
                () -> IngestTopics.of(com.legend.Compiler.compileModel(MODEL), "t::Nope"));
        assertTrue(none.getMessage().contains("declares no such Database"), none.getMessage());
    }
}
