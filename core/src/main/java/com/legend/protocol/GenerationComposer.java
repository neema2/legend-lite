// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###FileGeneration}'s and {@code ###GenerationSpecification}'s elements as upstream prints them
 * ({@code GenerationGrammarComposerExtension} and its helpers).
 */
final class GenerationComposer {

    private GenerationComposer() {
    }

    static String fileGeneration(Json.Obj g) {
        StringBuilder b = new StringBuilder();
        String type = str(g, "type");
        if (type != null && !type.isEmpty()) {
            b.append(type.substring(0, 1).toUpperCase(Locale.ROOT)).append(type.substring(1));
        }
        b.append(" ").append(elementPath(g)).append("\n{\n");
        List<String> scope = new ArrayList<>();
        for (Json.Node n : items(g, "scopeElements")) {
            scope.add(DatabaseComposer.pointerPath(n));
        }
        if (!scope.isEmpty()) {
            b.append(TAB).append("scopeElements: [").append(String.join(", ", scope)).append("];\n");
        }
        String outputPath = str(g, "generationOutputPath");
        if (outputPath != null) {
            b.append(TAB).append("generationOutputPath: ").append(convertString(outputPath, true)).append(";\n");
        }
        List<String> properties = new ArrayList<>();
        for (Json.Obj p : objs(g, "configurationProperties")) {
            properties.add(TAB + p.getString("name") + ": " + renderObject(p.get("value")) + ";");
        }
        if (!properties.isEmpty()) {
            b.append(String.join("\n", properties)).append("\n");
        }
        return b.append("}").toString();
    }

    /** {@code PureGrammarComposerUtility.renderObject}: a configuration value as Java prints the deserialized object. */
    private static String renderObject(Json.Node v) {
        if (v instanceof Json.Str s) {
            return "'" + s.value() + "'";
        }
        if (v instanceof Json.Arr a) {
            List<String> out = new ArrayList<>();
            for (Json.Node n : a.items()) {
                out.add(renderObject(n));
            }
            return "[" + String.join(", ", out) + "]";
        }
        if (v instanceof Json.Obj o) {
            StringBuilder b = new StringBuilder("{\n");
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                b.append(tab(2)).append(e.getKey()).append(": ").append(renderObject(e.getValue())).append(";\n");
            }
            return b.append(TAB).append("}").toString();
        }
        if (v instanceof Json.Null) {
            return "null";
        }
        return RelationalConnectionComposer.raw(v);
    }

    static String generationSpecification(Json.Obj g) {
        List<String> nodes = new ArrayList<>();
        for (Json.Obj n : objs(g, "generationNodes")) {
            String id = str(n, "id");
            String element = DatabaseComposer.pointerPath(n.get("generationElement"));
            nodes.add(tab(2) + "{\n"
                    + (id != null && !id.equals(element) ? tab(3) + "id: " + convertString(id, true) + ";\n" : "")
                    + tab(3) + "generationElement: " + element + ";\n"
                    + tab(2) + "}");
        }
        List<String> files = new ArrayList<>();
        for (Json.Node n : items(g, "fileGenerations")) {
            files.add(tab(2) + DatabaseComposer.pointerPath(n));
        }
        return "GenerationSpecification " + elementPath(g) + "\n{\n"
                + (nodes.isEmpty() ? "" : "  generationNodes: [\n" + String.join(",\n", nodes) + "\n  ];\n")
                + (files.isEmpty() ? "" : TAB + "fileGenerations: [\n" + String.join(",\n", files) + "\n" + TAB + "];\n")
                + "}";
    }
}
