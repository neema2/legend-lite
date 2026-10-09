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
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###ExternalFormat}'s schema set and binding, and {@code ###Text}'s text, as upstream prints them
 * ({@code ExternalFormatGrammarComposerExtension}, {@code TextGrammarComposerExtension}) -- over the records
 * ({@link Protocol.PSchemaSet}, {@link Protocol.PBinding}, {@link Protocol.PText}; the protocol program's leg 2,
 * step 3).
 */
final class ExternalFormatComposer {

    private ExternalFormatComposer() {
    }

    /** The section's two kinds: schema sets and bindings. */
    static String element(Protocol.Element e) {
        return switch (e) {
            case Protocol.PSchemaSet s -> schemaSet(s);
            case Protocol.PBinding b -> binding(b);
            default -> throw Composing.refused("no ExternalFormat printer for a " + e.getClass().getSimpleName());
        };
    }

    /** {@link #element(Protocol.Element)} of the JSON, read first. */
    static String element(Json.Obj e) {
        return element(Composing.element(e, Protocol.Element.class));
    }

    private static String schemaSet(Protocol.PSchemaSet s) {
        StringBuilder b = new StringBuilder("SchemaSet ").append(Composing.elementPath(s.pkg(), s.name())).append("\n{\n")
                .append(TAB).append("format: ").append(s.format()).append(";\n")
                .append(TAB).append("schemas: [\n");
        List<Protocol.PSchema> schemas = s.schemas();
        for (int i = 0; i < schemas.size(); i++) {
            Protocol.PSchema schema = schemas.get(i);
            b.append(tab(2)).append("{\n");
            if (schema.id() != null) {
                b.append(tab(3)).append("id: ").append(convertIdentifier(schema.id())).append(";\n");
            }
            if (schema.location() != null) {
                b.append(tab(3)).append("location: ").append(convertString(schema.location(), true)).append(";\n");
            }
            b.append(tab(3)).append("content: ").append(convertString(schema.content(), true)).append(";\n")
                    .append(tab(2)).append("}").append(i < schemas.size() - 1 ? ",\n" : "\n");
        }
        return b.append(TAB).append("];\n}").toString();
    }

    private static String binding(Protocol.PBinding binding) {
        StringBuilder b = new StringBuilder("Binding ").append(Composing.elementPath(binding.pkg(), binding.name()))
                .append("\n{\n");
        if (binding.schemaSet() != null) {
            b.append(TAB).append("schemaSet: ").append(convertPath(binding.schemaSet())).append(";\n");
            if (binding.schemaId() != null) {
                b.append(TAB).append("schemaId: ").append(convertIdentifier(binding.schemaId())).append(";\n");
            }
        }
        b.append(TAB).append("contentType: ").append(convertString(binding.contentType(), true)).append(";\n");
        b.append(TAB).append("modelIncludes: [\n").append(String.join(",\n", paths(binding.modelIncludes()))).append("\n")
                .append(TAB).append("];\n");
        List<String> excludes = paths(binding.modelExcludes());
        if (!excludes.isEmpty()) {
            b.append(TAB).append("modelExcludes: [\n").append(String.join(",\n", excludes)).append("\n").append(TAB).append("];\n");
        }
        return b.append("}").toString();
    }

    private static List<String> paths(List<String> paths) {
        List<String> out = new ArrayList<>();
        for (String p : paths) {
            out.add(tab(2) + convertPath(p));
        }
        return out;
    }

    static String text(Protocol.PText t) {
        return "Text " + Composing.elementPath(t.pkg(), t.name()) + "\n{\n"
                + (t.type() != null ? TAB + "type: " + t.type() + ";\n" : "")
                + TAB + "content: " + convertString(t.content(), true) + ";\n"
                + "}";
    }

    /** {@link #text(Protocol.PText)} of the JSON, read first. */
    static String text(Json.Obj t) {
        return text(Composing.element(t, Protocol.PText.class));
    }
}
