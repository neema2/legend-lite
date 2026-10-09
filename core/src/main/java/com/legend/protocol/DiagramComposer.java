// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Diagram}'s diagram as upstream prints it ({@code DiagramGrammarComposerExtension.renderDiagram}) -- over
 * the record ({@link Protocol.PDiagram}; the protocol program's leg 2, step 3).
 */
final class DiagramComposer {

    private DiagramComposer() {
    }

    static String diagram(Protocol.PDiagram d) {
        StringBuilder b = new StringBuilder("Diagram ").append(Composing.elementPath(d.pkg(), d.name())).append("\n{\n");
        for (Protocol.PClassView v : d.classViews()) {
            b.append(TAB).append("classView ").append(v.id()).append("\n").append(TAB).append("{\n")
                    .append(tab(2)).append("class: ").append(convertPath(v.classPath())).append(";\n")
                    .append(tab(2)).append("position: ").append(point(v.x(), v.y())).append(";\n")
                    .append(tab(2)).append("rectangle: (").append(Double.toString(v.width())).append(",")
                    .append(Double.toString(v.height())).append(");\n");
            if (Boolean.TRUE.equals(v.hideProperties())) {
                b.append(tab(2)).append("hideProperties: true;\n");
            }
            if (Boolean.TRUE.equals(v.hideTaggedValues())) {
                b.append(tab(2)).append("hideTaggedValue: true;\n");
            }
            if (Boolean.TRUE.equals(v.hideStereotypes())) {
                b.append(tab(2)).append("hideStereotype: true;\n");
            }
            b.append(TAB).append("}\n");
        }
        for (Protocol.PPropertyView v : d.propertyViews()) {
            b.append(TAB).append("propertyView\n").append(TAB).append("{\n")
                    .append(tab(2)).append("property: ").append(convertPath(v.propertyClass())).append(".")
                    .append(convertIdentifier(v.propertyName())).append(";\n");
            edge(b, v.sourceView(), v.targetView(), v.points());
        }
        for (Protocol.PGeneralizationView v : d.generalizationViews()) {
            b.append(TAB).append("generalizationView\n").append(TAB).append("{\n");
            edge(b, v.sourceView(), v.targetView(), v.points());
        }
        return b.append("}").toString();
    }

    /** {@link #diagram(Protocol.PDiagram)} of the JSON, read first. */
    static String diagram(Json.Obj d) {
        return diagram(Composing.element(d, Protocol.PDiagram.class));
    }

    private static void edge(StringBuilder b, String source, String target, List<Protocol.PDiagramPoint> line) {
        List<String> points = new ArrayList<>();
        for (Protocol.PDiagramPoint p : line) {
            points.add(point(p.x(), p.y()));
        }
        b.append(tab(2)).append("source: ").append(source).append(";\n")
                .append(tab(2)).append("target: ").append(target).append(";\n")
                .append(tab(2)).append("points: [").append(String.join(",", points)).append("];\n")
                .append(TAB).append("}\n");
    }

    /** A point: each coordinate a Java double's {@code toString}. */
    private static String point(double x, double y) {
        return "(" + x + "," + y + ")";
    }
}
