// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Elasticsearch}'s cluster store and its connection as upstream prints them
 * ({@code HelperElasticsearchStoreComposer}, {@code HelperElasticsearchPropertyComposer}).
 */
final class ElasticsearchComposer {

    /** A property's keyword, by the union member its wire carries. */
    private static final Map<String, String> PROPERTY_TYPES = Map.ofEntries(
            Map.entry("keyword", "Keyword"), Map.entry("text", "Text"), Map.entry("date", "Date"),
            Map.entry("_short", "Short"), Map.entry("_byte", "Byte"), Map.entry("integer", "Integer"),
            Map.entry("_long", "Long"), Map.entry("_float", "Float"), Map.entry("half_float", "HalfFloat"),
            Map.entry("_double", "Double"), Map.entry("_boolean", "Boolean"), Map.entry("object", "Object"),
            Map.entry("nested", "Nested"));

    /** The settings upstream's printer asserts are absent: present, it throws. */
    private static final List<String> UNSUPPORTED = List.of("ignore_above", "dynamic", "similarity", "store", "doc_values");

    private ElasticsearchComposer() {
    }

    static String store(Json.Obj store) {
        List<String> indices = new ArrayList<>();
        for (Json.Obj index : objs(store, "indices")) {
            List<String> properties = new ArrayList<>();
            for (Json.Obj p : objs(index, "properties")) {
                properties.add(tab(4) + convertIdentifier(p.getString("propertyName")) + ": " + property(p.getObj("property"), 4));
            }
            indices.add(tab(2) + convertIdentifier(index.getString("indexName")) + ": {\n"
                    + tab(3) + "properties: [\n" + joinLines(properties)
                    + tab(3) + "];\n" + tab(2) + "}");
        }
        return "Elasticsearch7Cluster " + elementPath(store) + "\n{\n" + TAB + "indices: [\n" + joinLines(indices) + TAB + "];\n}\n";
    }

    /** Upstream's writer: each item, then {@code ,} between items, and a newline after each. */
    private static String joinLines(List<String> items) {
        return items.isEmpty() ? "" : String.join(",\n", items) + "\n";
    }

    /** A property (a union: one member names its kind) at the visitor's indent level. */
    private static String property(Json.Obj union, int level) {
        if (union.fields().size() != 1) {
            throw Composing.refused("an Elasticsearch property union with " + union.fields().size() + " members");
        }
        Map.Entry<String, Json.Node> member = union.fields().entrySet().iterator().next();
        String keyword = PROPERTY_TYPES.get(member.getKey());
        if (keyword == null) {
            throw Composing.refused("no composer rule for an Elasticsearch property of kind '" + member.getKey() + "'");
        }
        Json.Obj p = Composing.obj(member.getValue(), "property");
        for (String key : UNSUPPORTED) {
            if (Composing.value(p, key) != null) {
                throw Composing.refused("an Elasticsearch property with '" + key + "' (upstream's printer does not support it)");
            }
        }
        if (!Composing.items(p, "copy_to").isEmpty() || !fields(p, "meta").isEmpty()) {
            throw Composing.refused("an Elasticsearch property with copy_to or meta (upstream's printer does not support them)");
        }
        Map<String, Json.Node> properties = fields(p, "properties");
        Map<String, Json.Node> fields = fields(p, "fields");
        if (properties.isEmpty() && fields.isEmpty()) {
            return keyword;
        }
        return keyword + " {\n" + members("properties", properties, level + 1) + members("fields", fields, level + 1) + tab(level) + "}";
    }

    private static Map<String, Json.Node> fields(Json.Obj p, String key) {
        Json.Obj o = objOr(p, key);
        return o == null ? Map.of() : new TreeMap<>(o.fields());
    }

    /** {@code render(grammarName, properties)}: the members sorted by name, at {@code level}. */
    private static String members(String name, Map<String, Json.Node> members, int level) {
        if (members.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Json.Node> e : members.entrySet()) {
            out.add(tab(level + 1) + convertIdentifier(e.getKey()) + ": " + property(Composing.obj(e.getValue(), "property"), level + 1));
        }
        return tab(level) + name + ": [\n" + joinLines(out) + tab(level) + "];\n";
    }

    static String connection(Json.Obj c, String i) {
        return i + "{\n"
                + i + TAB + "store: " + convertPath(c.getString("element")) + ";\n"
                + i + TAB + "clusterDetails: # URL { " + c.getObj("sourceSpec").getString("url") + " }#;\n"
                + i + TAB + "authentication: " + AuthenticationComposer.authentication(c.getObj("authSpec"), 1, i) + ";\n"
                + i + "}";
    }
}
