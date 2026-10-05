// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * Embedded data ({@code Kind #{ ... }#}) as upstream prints it ({@code HelperEmbeddedDataGrammarComposer}
 * with core's and the stores' embedded data composers: external format, reference, model store, relation
 * elements, relational CSV, service store).
 */
final class EmbeddedDataComposer {

    /** An embedded data kind's keyword and its content printer, given the content's indentation. */
    record Kind(String keyword, BiFunction<Json.Obj, String, String> content) {
    }

    private static final String PARENTHESES = "()";
    private static final String BRACKETS = "[]";

    private static final Map<String, Kind> KINDS = Map.of(
            "externalFormat", new Kind("ExternalFormat", EmbeddedDataComposer::externalFormat),
            "modelStore", new Kind("ModelStore", EmbeddedDataComposer::modelStore),
            "relationAccessor", new Kind("Relation", EmbeddedDataComposer::relationElements),
            "relationalCSVData", new Kind("Relational", EmbeddedDataComposer::relationalCsv),
            "serviceStore", new Kind("ServiceStore", ServiceStoreComposer::embeddedData));

    private EmbeddedDataComposer() {
    }

    /** {@code composeEmbeddedData} at the context's indentation {@code i}. */
    static String compose(Json.Obj data, String i) {
        String inner = i + TAB;
        String keyword;
        String content;
        if ("reference".equals(Composing.type(data))) {
            Json.Obj element = data.getObj("dataElement");
            keyword = "DATASPACE".equals(str(element, "type")) ? "DataspaceTestData" : "Reference";
            content = inner + Composing.convertPath(element.getString("path"));
        } else {
            Kind kind = KINDS.get(Composing.type(data));
            if (kind == null) {
                throw Composing.refused("no composer rule for embedded data of _type '" + Composing.type(data) + "'");
            }
            keyword = kind.keyword();
            content = kind.content().apply(data, inner);
        }
        return i + keyword + "\n" + i + "#{\n" + content + "\n" + i + "}#";
    }

    private static String externalFormat(Json.Obj d, String i) {
        return i + "contentType: " + convertString(d.getString("contentType"), true) + ";\n"
                + i + "data: " + convertString(d.getString("data"), true) + ";";
    }

    private static String relationalCsv(Json.Obj d, String i) {
        List<String> tables = new ArrayList<>();
        for (Json.Obj t : objs(d, "tables")) {
            StringBuilder b = new StringBuilder(i).append(t.getString("schema")).append(".").append(t.getString("table")).append(":");
            String values = str(t, "values");
            if (values != null) {
                List<String> lines = new ArrayList<>();
                for (String l : Composing.splitDroppingTrailingEmpties(values, '\n', (char) 0)) {
                    lines.add(i + TAB + convertString(l + "\n", true));
                }
                b.append("\n").append(String.join("+\n", lines));
            }
            tables.add(b.append(";").toString());
        }
        return String.join("\n\n", tables);
    }

    // ---------------------------------------------------------------------
    // Relation elements
    // ---------------------------------------------------------------------

    private static String relationElements(Json.Obj d, String i) {
        List<String> out = new ArrayList<>();
        for (Json.Obj e : objs(d, "relationElements")) {
            List<String> paths = e.getStringArrayOr("paths", List.of());
            out.add(paths.isEmpty() ? alignedRelation(e, i, true)
                    : i + String.join(".", paths) + ":\n" + alignedRelation(e, i + TAB, false));
        }
        return String.join("\n\n", out);
    }

    /** {@code renderAlignedRelationElement}: the columns and rows, padded to their widest value. */
    static String alignedRelation(Json.Obj element, String base, boolean standAlone) {
        String inner = base + TAB;
        List<String> columns = element.getStringArrayOr("columns", List.of());
        List<List<String>> rows = new ArrayList<>();
        for (Json.Obj r : objs(element, "rows")) {
            rows.add(r.getStringArrayOr("values", List.of()));
        }
        int[] widths = new int[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            widths[c] = columns.get(c).length();
        }
        for (List<String> row : rows) {
            for (int c = 0; c < columns.size() && c < row.size(); c++) {
                widths[c] = Math.max(widths[c], row.get(c).length());
            }
        }
        StringBuilder b = new StringBuilder();
        if (standAlone) {
            b.append(base).append("#{\n");
        }
        b.append(inner).append(alignedLine(columns, widths));
        if (rows.isEmpty()) {
            b.append(";");
            if (standAlone) {
                b.append("\n").append(base).append("}#");
            }
            return b.toString();
        }
        b.append("\n");
        for (int r = 0; r < rows.size(); r++) {
            List<String> row = rows.get(r);
            List<String> cells = new ArrayList<>();
            for (int c = 0; c < columns.size(); c++) {
                cells.add(c < row.size() ? row.get(c) : "");
            }
            b.append(inner).append(alignedLine(cells, widths));
            if (r < rows.size() - 1) {
                b.append("\n");
            } else {
                b.append(";");
                if (!standAlone) {
                    return b.toString();
                }
                b.append("\n");
            }
        }
        return b.append(base).append("}#").toString();
    }

    private static String alignedLine(List<String> cells, int[] widths) {
        StringBuilder b = new StringBuilder();
        for (int c = 0; c < cells.size(); c++) {
            if (c > 0) {
                b.append(", ");
            }
            String v = cells.get(c);
            b.append(c < cells.size() - 1 && v.length() < widths[c] ? v + " ".repeat(widths[c] - v.length()) : v);
        }
        return b.toString();
    }

    // ---------------------------------------------------------------------
    // Model store data (ModelStoreDataGrammarComposer)
    // ---------------------------------------------------------------------

    private static String modelStore(Json.Obj d, String i) {
        List<String> out = new ArrayList<>();
        for (Json.Obj m : objs(d, "modelData")) {
            out.add(modelTestData(m, i));
        }
        return String.join(",\n", out);
    }

    private static String modelTestData(Json.Obj data, String i) {
        String indent = i + TAB;
        StringBuilder b = new StringBuilder(i).append(data.getString("model")).append(":\n");
        String type = Composing.type(data);
        if ("modelEmbeddedData".equals(type)) {
            return b.append(compose(data.getObj("data"), indent)).toString();
        }
        if (!"modelInstanceData".equals(type)) {
            throw Composing.refused("no composer rule for model data of _type '" + type + "'");
        }
        Json.Obj vs = data.getObj("instances");
        String vsType = Composing.type(vs);
        if ("packageableElementPtr".equals(vsType)) {
            java.util.LinkedHashMap<String, Json.Node> pointer = new java.util.LinkedHashMap<>();
            pointer.put("type", Json.str("DATA"));
            pointer.put("path", vs.get("fullPath"));
            java.util.LinkedHashMap<String, Json.Node> reference = new java.util.LinkedHashMap<>();
            reference.put("_type", Json.str("reference"));
            reference.put("dataElement", new Json.Obj(pointer));
            return b.append(compose(new Json.Obj(reference), indent)).toString();
        }
        if ("collection".equals(vsType) && items(vs, "values").size() == 1) {
            return b.append(indent).append("[\n").append(indent).append(TAB).append(modelValue(vs, BRACKETS, 2, i))
                    .append("\n").append(indent).append("]").toString();
        }
        return b.append(indent).append(modelValue(vs, BRACKETS, 1, i)).toString();
    }

    /**
     * One value of model data: upstream's visitor keeps a collection-style stack and an indent level as
     * mutable state; here they are the {@code style} and {@code level} arguments.
     */
    private static String modelValue(Json.Node node, String style, int level, String i) {
        Json.Obj vs = Composing.obj(node, "model data value");
        String type = Composing.type(vs);
        if ("collection".equals(type)) {
            List<Json.Node> values = items(vs, "values");
            boolean oneLine = values.size() <= 1 || PRIMITIVES.contains(Composing.type(Composing.obj(values.get(0), "value")));
            return collection(values, oneLine, style, level, i);
        }
        if ("func".equals(type) && "new".equals(str(vs, "function"))) {
            List<Json.Node> params = items(vs, "parameters");
            return "^" + Composing.obj(params.get(0), "class").getString("fullPath") + modelValue(params.get(2), PARENTHESES, level, i);
        }
        if ("keyExpression".equals(type)) {
            return Composing.obj(vs.get("key"), "key").getString("value") + " = " + modelValue(vs.get("expression"), BRACKETS, level, i);
        }
        return modelLiteral(vs, type);
    }

    private static final java.util.Set<String> PRIMITIVES = java.util.Set.of("string", "boolean", "integer", "float",
            "decimal", "dateTime", "strictDate", "strictTime", "latestDate");

    private static String modelLiteral(Json.Obj vs, String type) {
        Json.Node v = vs.getOr("value", null);
        if ("string".equals(type)) {
            return convertString(vs.getString("value"), true);
        }
        if ("dateTime".equals(type) || "strictDate".equals(type) || "strictTime".equals(type)) {
            String d = vs.getString("value");
            return d.indexOf('%') != -1 ? d : "%" + d;
        }
        if ("boolean".equals(type)) {
            return String.valueOf(vs.getBool("value"));
        }
        if ("enumValue".equals(type)) {
            return Composing.convertPath(vs.getString("fullPath")) + "." + Composing.convertIdentifier(vs.getString("value"));
        }
        if (v instanceof Json.Num n) {
            if ("integer".equals(type) && n.isInteger()) {
                return Long.toString(n.longValue());
            }
            if ("float".equals(type)) {
                return Double.toString(n.doubleValue());
            }
            if ("decimal".equals(type)) {
                BigDecimal d = n.decimalValue() != null ? n.decimalValue() : BigDecimal.valueOf(n.longValue());
                return d.toPlainString() + "D";
            }
        }
        throw Composing.refused("no model data rule for a value of _type '" + type + "'");
    }

    /** {@code formatCollection}. */
    private static String collection(List<Json.Node> values, boolean oneLine, String style, int level, String i) {
        if (values.isEmpty()) {
            return style;
        }
        if (values.size() == 1 && BRACKETS.equals(style)) {
            return modelValue(values.get(0), BRACKETS, level, i);
        }
        String newline = "\n" + i + tab(level + 1);
        List<String> out = new ArrayList<>();
        for (Json.Node v : values) {
            out.add(modelValue(v, BRACKETS, level + 1, i));
        }
        return style.charAt(0) + (oneLine ? "" : newline) + String.join(oneLine ? ", " : "," + newline, out)
                + (oneLine ? "" : "\n" + i + tab(level)) + style.charAt(1);
    }
}
