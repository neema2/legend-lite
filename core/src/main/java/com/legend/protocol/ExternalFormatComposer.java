// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###ExternalFormat}'s schema set and binding, and {@code ###Text}'s text, as upstream prints them
 * ({@code ExternalFormatGrammarComposerExtension}, {@code TextGrammarComposerExtension}).
 */
final class ExternalFormatComposer {

    private ExternalFormatComposer() {
    }

    /** The section's two kinds: schema sets and bindings. */
    static String element(Json.Obj e) {
        return "binding".equals(Composing.type(e)) ? binding(e) : schemaSet(e);
    }

    private static String schemaSet(Json.Obj s) {
        StringBuilder b = new StringBuilder("SchemaSet ").append(elementPath(s)).append("\n{\n")
                .append(TAB).append("format: ").append(s.getString("format")).append(";\n")
                .append(TAB).append("schemas: [\n");
        List<Json.Obj> schemas = objs(s, "schemas");
        for (int i = 0; i < schemas.size(); i++) {
            Json.Obj schema = schemas.get(i);
            b.append(tab(2)).append("{\n");
            String id = str(schema, "id");
            if (id != null) {
                b.append(tab(3)).append("id: ").append(convertIdentifier(id)).append(";\n");
            }
            String location = str(schema, "location");
            if (location != null) {
                b.append(tab(3)).append("location: ").append(convertString(location, true)).append(";\n");
            }
            b.append(tab(3)).append("content: ").append(convertString(schema.getString("content"), true)).append(";\n")
                    .append(tab(2)).append("}").append(i < schemas.size() - 1 ? ",\n" : "\n");
        }
        return b.append(TAB).append("];\n}").toString();
    }

    private static String binding(Json.Obj binding) {
        StringBuilder b = new StringBuilder("Binding ").append(elementPath(binding)).append("\n{\n");
        String schemaSet = str(binding, "schemaSet");
        if (schemaSet != null) {
            b.append(TAB).append("schemaSet: ").append(convertPath(schemaSet)).append(";\n");
            String schemaId = str(binding, "schemaId");
            if (schemaId != null) {
                b.append(TAB).append("schemaId: ").append(convertIdentifier(schemaId)).append(";\n");
            }
        }
        b.append(TAB).append("contentType: ").append(convertString(binding.getString("contentType"), true)).append(";\n");
        Json.Obj unit = binding.getObj("modelUnit");
        b.append(TAB).append("modelIncludes: [\n").append(String.join(",\n", paths(unit, "packageableElementIncludes"))).append("\n")
                .append(TAB).append("];\n");
        List<String> excludes = paths(unit, "packageableElementExcludes");
        if (!excludes.isEmpty()) {
            b.append(TAB).append("modelExcludes: [\n").append(String.join(",\n", excludes)).append("\n").append(TAB).append("];\n");
        }
        return b.append("}").toString();
    }

    private static List<String> paths(Json.Obj unit, String key) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : items(unit, key)) {
            out.add(tab(2) + convertPath(DatabaseComposer.pointerPath(n)));
        }
        return out;
    }

    static String text(Json.Obj t) {
        String type = str(t, "type");
        return "Text " + elementPath(t) + "\n{\n"
                + (type != null ? TAB + "type: " + type + ";\n" : "")
                + TAB + "content: " + convertString(t.getString("content"), true) + ";\n"
                + "}";
    }
}
