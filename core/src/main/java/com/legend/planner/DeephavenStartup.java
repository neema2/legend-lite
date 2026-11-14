// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.RuntimeDefinition;
import com.legend.sql.dialect.DeephavenProgram;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A DEEPHAVEN SERVER'S STARTUP PROGRAM, FROM THE MODEL (docs/DESK_DATA_ROADMAP_2026_10_29.md B2, the user's go of
 * 2026-11-14): for each store a runtime binds to a Deephaven connection, the tables its streams land -- each dataset's
 * topic (IngestTopics), read from the brokers the same runtime binds the topic's store to (a Kafka connection), kept as
 * the dataset lands -- rendered by the Deephaven dialect's program writer (DeephavenProgram). The model's text alone:
 * no row and no value; the server runs it at start, and again after a restart, so every table is made again.
 */
public final class DeephavenStartup {

    private DeephavenStartup() {
    }

    /** The startup program of the Deephaven servers {@code runtime} names, as Groovy for Deephaven's application. */
    public static String program(ModelContext ctx, String runtime) {
        RuntimeDefinition r = ctx.findRuntime(runtime)
                .orElseThrow(() -> new IllegalArgumentException("the startup program of " + runtime
                        + ": the model declares no such Runtime"));
        List<DeephavenProgram.Subscription> subscriptions = new ArrayList<>();
        for (Map.Entry<String, List<String>> binding : r.connectionBindings().entrySet()) {
            for (String connection : binding.getValue()) {
                // a server whose tables a startup program makes (the dialect says: Deephaven's): the store it serves is
                // the server's tables
                ConnectionDefinition.DatabaseType type = connection(ctx, connection).databaseType();
                boolean planned = com.legend.database.Databases.implemented().contains(type)
                        || com.legend.database.Databases.hostExecuted().contains(type);
                if (!planned || !com.legend.database.Databases.dialect(type).tablesMadeAtStartup()) {
                    continue;
                }
                for (IngestTopics.Topic t : IngestTopics.of(ctx, binding.getKey())) {
                    ConnectionSpecification.Kafka kafka = kafka(ctx, r, t);
                    subscriptions.add(new DeephavenProgram.Subscription(t.table(), t.topic(), kafka.bootstrapServers(),
                            kafka.format(), t.columns().stream()
                                    .map(c -> new DeephavenProgram.Column(c.name(), c.type())).toList(),
                            t.key(), t.keepRows(), t.keepFor()));
                }
            }
        }
        return DeephavenProgram.render(subscriptions);
    }

    /** The brokers {@code runtime} binds {@code topic}'s store to: one Kafka connection, else refused by name. */
    private static ConnectionSpecification.Kafka kafka(ModelContext ctx, RuntimeDefinition runtime,
            IngestTopics.Topic topic) {
        List<String> bound = runtime.connectionBindings().getOrDefault(topic.store(), List.of());
        List<ConnectionSpecification.Kafka> kafka = new ArrayList<>();
        for (String c : bound) {
            if (connection(ctx, c).specification() instanceof ConnectionSpecification.Kafka k) {
                kafka.add(k);
            }
        }
        if (kafka.size() != 1) {
            throw new IllegalArgumentException("the table " + topic.table() + " reads the topic " + topic.topic()
                    + " of " + topic.store() + ", which the runtime " + runtime.qualifiedName() + " binds to "
                    + (kafka.isEmpty() ? "no Kafka connection" : kafka.size() + " Kafka connections") + ": a stream's"
                    + " brokers are one Kafka connection of the runtime it lands in");
        }
        return kafka.get(0);
    }

    private static ConnectionDefinition connection(ModelContext ctx, String fqn) {
        return ctx.findConnection(fqn).orElseThrow(() -> new IllegalArgumentException("the model declares no"
                + " connection " + fqn));
    }
}
