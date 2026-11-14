// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import com.legend.model.IngestDefinition;
import com.legend.protocol.Protocol;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * THE TOPICS A DATABASE'S STREAMS READ (docs/DESK_DATA_ROADMAP_2026_10_29.md B1, the user's go of 2026-11-14): each
 * dataset of a {@code Stream} ingest the database reaches through its includes, as the topic its rows arrive on -- the
 * one fact the source side needs (the world's feed writes each table's rows to its topic; the door keeps each topic as
 * its dataset lands) and the one B2's generated program subscribes by. All of it is the model's: the topic is the
 * topic store's table the dataset reads ({@code |#>{tickerplant::Topics."tp.us-equities.trades.v3"}#}), or, for a
 * dataset whose columns are written out, the ingest's own topic -- its path, a dot for each {@code ::}, then the
 * dataset ({@code market.WorldStatus.world}); its columns are the landing table's, in its order, each with the Java
 * type a Deephaven column holds it as (DH16); a dataset that lands latest by key is a compacted topic, keyed by the
 * dataset's key; an appended one keeps its retention. Pulled and filed datasets are not topics (B3). A topic name
 * Kafka cannot take is refused by name.
 */
public final class IngestTopics {

    private IngestTopics() {
    }

    /** Kafka's topic names: letters, digits, dots, underscores and hyphens, at most 249. */
    private static final Pattern TOPIC = Pattern.compile("[A-Za-z0-9._-]{1,249}");

    /** A column of a topic's records: its name, and the Java type a Deephaven column holds it as. */
    public record Column(String name, String type) {
    }

    /**
     * One topic: the landing table it feeds ({@code table}), its name, its records' columns, the key a compacted
     * topic keeps each latest record of (empty: an appended topic), and an appended topic's retention -- its latest
     * {@code keepRows} records, or those of the last {@code keepFor} (an ISO 8601 duration) -- null for none.
     */
    public record Topic(String table, String topic, List<Column> columns, List<String> key,
            @com.legend.base.Nullable Long keepRows, @com.legend.base.Nullable String keepFor) {
        public Topic {
            columns = List.copyOf(columns);
            key = List.copyOf(key);
        }
    }

    /** The topics {@code database}'s streams read, in its includes' order, each dataset's in its ingest's. */
    public static List<Topic> of(ModelContext ctx, String database) {
        if (ctx.findDatabase(database).isEmpty()) {
            throw new IllegalArgumentException("the topics of " + database + ": the model declares no such Database");
        }
        List<Topic> topics = new ArrayList<>();
        ArrayDeque<String> next = new ArrayDeque<>(List.of(database));
        Set<String> seen = new HashSet<>();
        while (!next.isEmpty()) {
            String name = next.poll();
            if (!seen.add(name)) {
                continue;
            }
            var ingest = ctx.findIngest(name);
            if (ingest.isPresent()) {
                if (ingest.get().ingest().readMode() instanceof Protocol.PIngestStream) {
                    for (Protocol.PIngestDataset d : ingest.get().ingest().datasets()) {
                        topics.add(topic(ctx, ingest.get(), d));
                    }
                }
                continue;
            }
            ctx.findDatabase(name).ifPresent(db -> next.addAll(db.includes()));
        }
        return topics;
    }

    private static Topic topic(ModelContext ctx, IngestDefinition ingest, Protocol.PIngestDataset d) {
        String where = "the ingest " + ingest.qualifiedName() + "'s dataset " + d.name();
        IngestDefinition.TableRead read = IngestDefinition.tableRead(d);
        String name = read != null ? read.table() : ingest.qualifiedName().replace("::", ".") + "." + d.name();
        if (!TOPIC.matcher(name).matches()) {
            throw new IllegalArgumentException(where + " reads the topic '" + name + "', which is no Kafka topic's name"
                    + " (letters, digits, '.', '_' and '-', at most 249)");
        }
        DatabaseDefinition.TableDefinition table = ctx.findOwnTableDefinition(ingest.qualifiedName(), d.name())
                .orElseThrow(() -> new IllegalStateException(where + " makes no table"));
        List<Column> columns = table.columns().stream().map(c -> new Column(c.name(),
                com.legend.sql.dialect.Deephaven.columnJavaType(
                        com.legend.compiler.element.StoreCompiler.declaredType(c.dataType()), d.name() + "." + c.name())))
                .toList();
        String mode = ingest.writeModeOf(d);
        boolean keyed = "latest_by_key".equals(mode);
        if (!keyed && !"append_only".equals(mode)) {
            throw new IllegalArgumentException(where + " lands " + (mode == null ? "by no write mode" : "as " + mode)
                    + ": a topic is appended (AppendOnly) or keeps the latest record of each key (LatestByKey)");
        }
        Long rows = null;
        String duration = null;
        if (d.retention() != null) {
            switch (d.retention()) {
                case Protocol.PIngestLastRows r -> rows = r.rows();
                case Protocol.PIngestLast l -> duration = l.duration();
            }
        }
        return new Topic(d.name(), name, columns, keyed ? d.primaryKey() : List.of(), rows, duration);
    }
}
