// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Connection}'s connections and the connection values a runtime embeds, as upstream prints them
 * ({@code DEPRECATED_PureGrammarComposerCore.visit(PackageableConnection)}, its model connections, and each
 * store extension's connection value composer).
 */
final class ConnectionComposer {

    /** A connection value kind: the keyword it is declared with, and its body printed at an indentation. */
    record Kind(String keyword, BiFunction<Json.Obj, String, String> body) {
    }

    private static final Map<String, Kind> KINDS = Map.of(
            "JsonModelConnection", new Kind("JsonModelConnection", (c, i) -> modelConnection(c, i)),
            "XmlModelConnection", new Kind("XmlModelConnection", (c, i) -> modelConnection(c, i)),
            "ModelChainConnection", new Kind("ModelChainConnection", ConnectionComposer::modelChain),
            "RelationalDatabaseConnection", new Kind("RelationalDatabaseConnection", RelationalConnectionComposer::connection));

    private ConnectionComposer() {
    }

    static String connection(Json.Obj connection) {
        Json.Obj value = connection.getObj("connectionValue");
        Kind kind = kind(value);
        return kind.keyword() + " " + elementPath(connection) + "\n" + kind.body().apply(value, "");
    }

    static Kind kind(Json.Obj value) {
        Kind kind = KINDS.get(Composing.type(value));
        if (kind == null) {
            throw Composing.refused("no composer rule for a connection of _type '" + Composing.type(value) + "'");
        }
        return kind;
    }

    private static String modelConnection(Json.Obj c, String i) {
        return i + "{\n"
                + i + TAB + "class: " + c.getString("class") + ";\n"
                + i + TAB + "url: " + convertString(c.getString("url"), true) + ";\n"
                + i + "}";
    }

    private static String modelChain(Json.Obj c, String i) {
        List<String> mappings = new ArrayList<>();
        for (String m : c.getStringArrayOr("mappings", List.of())) {
            mappings.add(i + tab(2) + m);
        }
        return i + "{\n"
                + i + TAB + "mappings: [\n" + String.join(",\n", mappings) + (mappings.isEmpty() ? "" : "\n") + i + TAB + "];\n"
                + i + "}";
    }
}
