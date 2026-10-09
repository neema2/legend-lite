// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Connection}'s connections and the connection values a runtime embeds, as upstream prints them
 * ({@code DEPRECATED_PureGrammarComposerCore.visit(PackageableConnection)}, its model connections, and each
 * store extension's connection value composer) -- over the records ({@link Protocol.PConnection},
 * {@link Protocol.PConnectionValue}; the protocol program's leg 2, step 3).
 */
final class ConnectionComposer {

    private ConnectionComposer() {
    }

    static String connection(Protocol.PConnection connection) {
        return keyword(connection.value()) + " " + Composing.elementPath(connection.pkg(), connection.name()) + "\n"
                + body(connection.value(), "");
    }

    /** The keyword a connection value is declared (or embedded) with. */
    static String keyword(Protocol.PConnectionValue value) {
        return switch (value) {
            case Protocol.PJsonModelConnection c -> "JsonModelConnection";
            case Protocol.PXmlModelConnection c -> "XmlModelConnection";
            case Protocol.PModelChainConnection c -> "ModelChainConnection";
            case Protocol.PRelationalDatabaseConnection c -> "RelationalDatabaseConnection";
            case Protocol.PServiceStoreConnection c -> "ServiceStoreConnection";
            case Protocol.PElasticsearchConnection c -> "Elasticsearch7ClusterConnection";
            case Protocol.PMongoDbConnection c -> "MongoDBConnection";
            case Protocol.PDeephavenConnection c -> "DeephavenConnection";
            case Protocol.PConnectionPointer p -> throw pointer();
        };
    }

    /** A connection value's body, at the context's indentation {@code i}. */
    static String body(Protocol.PConnectionValue value, String i) {
        return switch (value) {
            case Protocol.PJsonModelConnection c -> modelConnection(c.className(), c.url(), i);
            case Protocol.PXmlModelConnection c -> modelConnection(c.className(), c.url(), i);
            case Protocol.PModelChainConnection c -> modelChain(c, i);
            case Protocol.PRelationalDatabaseConnection c -> RelationalConnectionComposer.connection(c, i);
            case Protocol.PServiceStoreConnection c -> ServiceStoreComposer.connection(c, i);
            case Protocol.PElasticsearchConnection c -> ElasticsearchComposer.connection(c, i);
            case Protocol.PMongoDbConnection c -> MongoComposer.connection(c, i);
            case Protocol.PDeephavenConnection c -> DeephavenComposer.connection(c, i);
            case Protocol.PConnectionPointer p -> throw pointer();
        };
    }

    /** A pointer is printed by its path where a connection may be one; a connection element is never one. */
    private static IllegalArgumentException pointer() {
        return Composing.refused("no composer rule for a connection of _type 'connectionPointer'");
    }

    private static String modelConnection(String className, String url, String i) {
        return i + "{\n"
                + i + TAB + "class: " + className + ";\n"
                + i + TAB + "url: " + convertString(url, true) + ";\n"
                + i + "}";
    }

    private static String modelChain(Protocol.PModelChainConnection c, String i) {
        List<String> mappings = new ArrayList<>();
        for (String m : c.mappings()) {
            mappings.add(i + tab(2) + m);
        }
        return i + "{\n"
                + i + TAB + "mappings: [\n" + String.join(",\n", mappings) + (mappings.isEmpty() ? "" : "\n") + i + TAB + "];\n"
                + i + "}";
    }
}
