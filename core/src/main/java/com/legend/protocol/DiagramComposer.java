// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Diagram}'s diagram as upstream prints it ({@code DiagramGrammarComposerExtension.renderDiagram}).
 */
final class DiagramComposer {

    private DiagramComposer() {
    }

    static String diagram(Json.Obj d) {
        StringBuilder b = new StringBuilder("Diagram ").append(elementPath(d)).append("\n{\n");
        for (Json.Obj v : objs(d, "classViews")) {
            b.append(TAB).append("classView ").append(v.getString("id")).append("\n").append(TAB).append("{\n")
                    .append(tab(2)).append("class: ").append(convertPath(v.getString("class"))).append(";\n")
                    .append(tab(2)).append("position: ").append(point(v.getObj("position"))).append(";\n")
                    .append(tab(2)).append("rectangle: (").append(number(v.getObj("rectangle"), "width")).append(",")
                    .append(number(v.getObj("rectangle"), "height")).append(");\n");
            if (v.getBoolOr("hideProperties", false)) {
                b.append(tab(2)).append("hideProperties: true;\n");
            }
            if (v.getBoolOr("hideTaggedValues", false)) {
                b.append(tab(2)).append("hideTaggedValue: true;\n");
            }
            if (v.getBoolOr("hideStereotypes", false)) {
                b.append(tab(2)).append("hideStereotype: true;\n");
            }
            b.append(TAB).append("}\n");
        }
        for (Json.Obj v : objs(d, "propertyViews")) {
            Json.Obj property = v.getObj("property");
            b.append(TAB).append("propertyView\n").append(TAB).append("{\n")
                    .append(tab(2)).append("property: ").append(convertPath(property.getString("class"))).append(".")
                    .append(convertIdentifier(property.getString("property"))).append(";\n");
            edge(b, v);
        }
        for (Json.Obj v : objs(d, "generalizationViews")) {
            b.append(TAB).append("generalizationView\n").append(TAB).append("{\n");
            edge(b, v);
        }
        return b.append("}").toString();
    }

    private static void edge(StringBuilder b, Json.Obj v) {
        List<String> points = new ArrayList<>();
        for (Json.Obj p : objs(v.getObj("line"), "points")) {
            points.add(point(p));
        }
        b.append(tab(2)).append("source: ").append(v.getString("sourceView")).append(";\n")
                .append(tab(2)).append("target: ").append(v.getString("targetView")).append(";\n")
                .append(tab(2)).append("points: [").append(String.join(",", points)).append("];\n")
                .append(TAB).append("}\n");
    }

    private static String point(Json.Obj p) {
        return "(" + number(p, "x") + "," + number(p, "y") + ")";
    }

    /** A coordinate: a Java double's {@code toString}. */
    private static String number(Json.Obj o, String key) {
        return Double.toString(Composing.doubleOf((Json.Num) o.get(key)));
    }
}
