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
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###FileGeneration}'s and {@code ###GenerationSpecification}'s elements as upstream prints them
 * ({@code GenerationGrammarComposerExtension} and its helpers) -- over the records ({@link Protocol.PFileGeneration},
 * {@link Protocol.PGenerationSpecification}; the protocol program's leg 2, step 3).
 */
final class GenerationComposer {

    private GenerationComposer() {
    }

    static String fileGeneration(Protocol.PFileGeneration g) {
        StringBuilder b = new StringBuilder();
        String type = g.type();
        if (!type.isEmpty()) {
            b.append(type.substring(0, 1).toUpperCase(Locale.ROOT)).append(type.substring(1));
        }
        b.append(" ").append(Composing.elementPath(g.pkg(), g.name())).append("\n{\n");
        if (!g.scopeElements().isEmpty()) {
            b.append(TAB).append("scopeElements: [").append(String.join(", ", g.scopeElements())).append("];\n");
        }
        if (g.generationOutputPath() != null) {
            b.append(TAB).append("generationOutputPath: ").append(convertString(g.generationOutputPath(), true)).append(";\n");
        }
        List<String> properties = new ArrayList<>();
        for (Protocol.PConfigProperty p : g.configurationProperties()) {
            properties.add(TAB + p.name() + ": " + renderObject(p.value()) + ";");
        }
        if (!properties.isEmpty()) {
            b.append(String.join("\n", properties)).append("\n");
        }
        return b.append("}").toString();
    }

    /** {@link #fileGeneration(Protocol.PFileGeneration)} of the JSON, read first. */
    static String fileGeneration(Json.Obj g) {
        return fileGeneration(Composing.element(g, Protocol.PFileGeneration.class));
    }

    /** {@code PureGrammarComposerUtility.renderObject}: a configuration value as Java prints the deserialized object. */
    private static String renderObject(Protocol.PConfigValue v) {
        return switch (v) {
            case Protocol.PConfigValue.PCString s -> quoted(s.value());
            case Protocol.PConfigValue.PCBoolean b -> String.valueOf(b.value());
            case Protocol.PConfigValue.PCInteger n -> Long.toString(n.value());
            case Protocol.PConfigValue.PCStrings l -> {
                List<String> out = new ArrayList<>();
                for (String s : l.values()) {
                    out.add(quoted(s));
                }
                yield "[" + String.join(", ", out) + "]";
            }
            case Protocol.PConfigValue.PCMap m -> {
                StringBuilder b = new StringBuilder("{\n");
                for (Map.Entry<String, String> e : m.entries().entrySet()) {
                    b.append(tab(2)).append(e.getKey()).append(": ").append(quoted(e.getValue())).append(";\n");
                }
                yield b.append(TAB).append("}").toString();
            }
        };
    }

    private static String quoted(String s) {
        return "'" + s + "'";
    }

    static String generationSpecification(Protocol.PGenerationSpecification g) {
        List<String> nodes = new ArrayList<>();
        for (Protocol.PGenerationNode n : g.generationNodes()) {
            String element = n.generationElement();
            nodes.add(tab(2) + "{\n"
                    + (!n.id().equals(element) ? tab(3) + "id: " + convertString(n.id(), true) + ";\n" : "")
                    + tab(3) + "generationElement: " + element + ";\n"
                    + tab(2) + "}");
        }
        List<String> files = new ArrayList<>();
        for (Protocol.PPointer p : g.fileGenerations()) {
            files.add(tab(2) + p.path());
        }
        return "GenerationSpecification " + Composing.elementPath(g.pkg(), g.name()) + "\n{\n"
                + (nodes.isEmpty() ? "" : "  generationNodes: [\n" + String.join(",\n", nodes) + "\n  ];\n")
                + (files.isEmpty() ? "" : TAB + "fileGenerations: [\n" + String.join(",\n", files) + "\n" + TAB + "];\n")
                + "}";
    }

    /** {@link #generationSpecification(Protocol.PGenerationSpecification)} of the JSON, read first. */
    static String generationSpecification(Json.Obj g) {
        return generationSpecification(Composing.element(g, Protocol.PGenerationSpecification.class));
    }
}
