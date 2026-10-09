// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Elasticsearch}'s cluster store and its connection as upstream prints them
 * ({@code HelperElasticsearchStoreComposer}, {@code HelperElasticsearchPropertyComposer}) -- over the records
 * ({@link Protocol.PElasticsearch7Cluster}, {@link Protocol.PElasticsearchConnection}; the protocol program's leg 2,
 * step 3). The settings upstream's printer does not support ({@code ignore_above}, {@code copy_to}, ...) have no
 * reader rule: such a property is refused when read.
 */
final class ElasticsearchComposer {

    /** A property's keyword, by the union member its wire carries. */
    private static final Map<String, String> PROPERTY_TYPES = Map.ofEntries(
            Map.entry("keyword", "Keyword"), Map.entry("text", "Text"), Map.entry("date", "Date"),
            Map.entry("_short", "Short"), Map.entry("_byte", "Byte"), Map.entry("integer", "Integer"),
            Map.entry("_long", "Long"), Map.entry("_float", "Float"), Map.entry("half_float", "HalfFloat"),
            Map.entry("_double", "Double"), Map.entry("_boolean", "Boolean"), Map.entry("object", "Object"),
            Map.entry("nested", "Nested"));

    private ElasticsearchComposer() {
    }

    static String store(Protocol.PElasticsearch7Cluster store) {
        List<String> indices = new ArrayList<>();
        for (Protocol.PElasticsearch7Cluster.PEsIndex index : store.indices()) {
            List<String> properties = new ArrayList<>();
            for (Protocol.PElasticsearch7Cluster.PEsProperty p : index.properties()) {
                properties.add(tab(4) + convertIdentifier(p.propertyName()) + ": " + property(p, 4));
            }
            indices.add(tab(2) + convertIdentifier(index.indexName()) + ": {\n"
                    + tab(3) + "properties: [\n" + joinLines(properties)
                    + tab(3) + "];\n" + tab(2) + "}");
        }
        return "Elasticsearch7Cluster " + Composing.elementPath(store.pkg(), store.name()) + "\n{\n" + TAB
                + "indices: [\n" + joinLines(indices) + TAB + "];\n}\n";
    }

    /** {@link #store(Protocol.PElasticsearch7Cluster)} of the JSON, read first. */
    static String store(Json.Obj store) {
        return store(Composing.element(store, Protocol.PElasticsearch7Cluster.class));
    }

    /** Upstream's writer: each item, then {@code ,} between items, and a newline after each. */
    private static String joinLines(List<String> items) {
        return items.isEmpty() ? "" : String.join(",\n", items) + "\n";
    }

    /** A property (its wire key names its kind) at the visitor's indent level. */
    private static String property(Protocol.PElasticsearch7Cluster.PEsProperty p, int level) {
        String keyword = PROPERTY_TYPES.get(p.wireKey());
        if (keyword == null) {
            throw Composing.refused("no composer rule for an Elasticsearch property of kind '" + p.wireKey() + "'");
        }
        List<Protocol.PElasticsearch7Cluster.PEsProperty> properties = sorted(p.childProperties());
        List<Protocol.PElasticsearch7Cluster.PEsProperty> fields = sorted(p.fields());
        if (properties.isEmpty() && fields.isEmpty()) {
            return keyword;
        }
        return keyword + " {\n" + members("properties", properties, level + 1) + members("fields", fields, level + 1)
                + tab(level) + "}";
    }

    /** A property's children sorted by name, as upstream's printer takes them from its map. */
    private static List<Protocol.PElasticsearch7Cluster.PEsProperty> sorted(
            @com.legend.base.Nullable List<Protocol.PElasticsearch7Cluster.PEsProperty> children) {
        if (children == null) {
            return List.of();
        }
        List<Protocol.PElasticsearch7Cluster.PEsProperty> out = new ArrayList<>(children);
        out.sort(Comparator.comparing(Protocol.PElasticsearch7Cluster.PEsProperty::propertyName));
        return out;
    }

    /** {@code render(grammarName, properties)}: the members, at {@code level}. */
    private static String members(String name, List<Protocol.PElasticsearch7Cluster.PEsProperty> members, int level) {
        if (members.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PElasticsearch7Cluster.PEsProperty m : members) {
            out.add(tab(level + 1) + convertIdentifier(m.propertyName()) + ": " + property(m, level + 1));
        }
        return tab(level) + name + ": [\n" + joinLines(out) + tab(level) + "];\n";
    }

    static String connection(Protocol.PElasticsearchConnection c, String i) {
        return i + "{\n"
                + i + TAB + "store: " + convertPath(c.element()) + ";\n"
                + i + TAB + "clusterDetails: # URL { " + c.url() + " }#;\n"
                + i + TAB + "authentication: " + AuthenticationComposer.authentication(c.auth(), 1, i) + ";\n"
                + i + "}";
    }

    /** {@link #connection(Protocol.PElasticsearchConnection, String)} of the JSON, read first. */
    static String connection(Json.Obj c, String i) {
        if (!(ConnectionReader.connectionValue(c) instanceof Protocol.PElasticsearchConnection r)) {
            throw Composing.refused("an Elasticsearch connection that reads as another kind");
        }
        return connection(r, i);
    }
}
