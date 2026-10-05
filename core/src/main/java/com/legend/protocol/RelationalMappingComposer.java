// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * A relational class mapping and association mapping as upstream prints them
 * ({@code RelationalGrammarComposerExtension}'s class and association mapping composers and
 * {@code HelperRelationalGrammarComposer}'s property mappings).
 */
final class RelationalMappingComposer {

    private RelationalMappingComposer() {
    }

    static String classMapping(Json.Obj cm) {
        RelationalOperations ops = RelationalOperations.mapping("");
        StringBuilder b = new StringBuilder(": Relational\n").append(TAB).append("{\n");
        Json.Obj filter = objOr(cm, "filter");
        if (filter != null) {
            b.append(tab(2)).append(RelationalOperations.filterMapping(filter)).append("\n");
        }
        if (cm.getBoolOr("distinct", false)) {
            b.append(tab(2)).append("~distinct\n");
        }
        operations(b, "~groupBy", objs(cm, "groupBy"), ops);
        operations(b, "~primaryKey", objs(cm, "primaryKey"), ops);
        Json.Obj mainTable = objOr(cm, "mainTable");
        if (mainTable != null) {
            String schema = str(mainTable, "schema");
            String table = mainTable.getString("table");
            b.append(tab(2)).append("~mainTable [").append(RelationalOperations.tableDb(mainTable)).append("]")
                    .append(schema != null && !"default".equals(schema) ? schema + "." + table : table).append("\n");
        }
        List<Json.Obj> pms = objs(cm, "propertyMappings");
        if (!pms.isEmpty()) {
            b.append(propertyMappings(pms, ops.indented(4), false)).append("\n");
        }
        return b.append(TAB).append("}").toString();
    }

    private static void operations(StringBuilder b, String keyword, List<Json.Obj> operations, RelationalOperations ops) {
        if (operations.isEmpty()) {
            return;
        }
        List<String> out = new ArrayList<>();
        for (Json.Obj op : operations) {
            out.add(tab(3) + ops.render(op));
        }
        b.append(tab(2)).append(keyword).append("\n").append(tab(2)).append("(\n")
                .append(String.join(",\n", out)).append("\n").append(tab(2)).append(")\n");
    }

    static String associationMapping(Json.Obj am, String association) {
        RelationalOperations ops = RelationalOperations.mapping("");
        List<Json.Obj> pms = objs(am, "propertyMappings");
        return association + ": Relational\n" + TAB + "{\n"
                + tab(2) + "AssociationMapping\n" + tab(2) + "(\n"
                + (pms.isEmpty() ? "" : propertyMappings(pms, ops.indented(6), true) + "\n")
                + tab(2) + ")\n" + TAB + "}";
    }

    private static String propertyMappings(List<Json.Obj> pms, RelationalOperations ops, boolean renderSourceId) {
        List<String> out = new ArrayList<>();
        for (Json.Obj pm : pms) {
            out.add(propertyMapping(pm, ops, renderSourceId));
        }
        return String.join(",\n", out);
    }

    /** {@code renderAbstractRelationalPropertyMapping}. */
    private static String propertyMapping(Json.Obj pm, RelationalOperations ops, boolean renderSourceId) {
        String type = Composing.type(pm);
        if ("relationalPropertyMapping".equals(type)) {
            return relationalPropertyMapping(pm, ops, renderSourceId);
        }
        if ("embeddedPropertyMapping".equals(type) || "otherwiseEmbeddedPropertyMapping".equals(type)) {
            return embedded(pm, ops);
        }
        if ("inlineEmbeddedPropertyMapping".equals(type)) {
            return ops.indentation() + property(pm) + "() Inline[" + convertIdentifier(pm.getString("setImplementationId")) + "]";
        }
        throw Composing.refused("no composer rule for a relational property mapping of _type '" + type + "'");
    }

    private static String property(Json.Obj pm) {
        return convertIdentifier(pm.getObj("property").getString("property"));
    }

    private static String relationalPropertyMapping(Json.Obj pm, RelationalOperations ops, boolean renderSourceId) {
        Json.Obj local = objOr(pm, "localMappingProperty");
        String target = str(pm, "target");
        String source = str(pm, "source");
        String head = local != null
                ? "+" + property(pm) + ": " + local.getString("type") + "[" + Composing.multiplicity(local.getObj("multiplicity")) + "]"
                : property(pm) + (empty(target) ? "" : "[" + (renderSourceId ? (empty(source) ? "" : source + ",") : "") + target + "]");
        String enumMapping = str(pm, "enumMappingId");
        Json.Obj binding = objOr(pm, "bindingTransformer");
        return ops.indentation() + head + ": "
                + (enumMapping != null ? "EnumerationMapping " + convertIdentifier(enumMapping) + ": " : "")
                + (enumMapping == null && binding != null ? "Binding " + Composing.convertPath(binding.getString("binding")) + " : " : "")
                + ops.render(pm.get("relationalOperation"));
    }

    private static boolean empty(@com.legend.base.Nullable String s) {
        return s == null || s.isEmpty();
    }

    /** {@code renderEmbeddedRelationalPropertyMapping} and its otherwise form. */
    private static String embedded(Json.Obj pm, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(ops.indentation()).append(property(pm)).append("\n").append(ops.indentation()).append("(\n");
        List<Json.Obj> nested = objs(pm.getObj("classMapping"), "propertyMappings");
        if (!nested.isEmpty()) {
            b.append(propertyMappings(nested, ops.indented(2), false)).append("\n");
        }
        b.append(ops.indentation()).append(")");
        if ("otherwiseEmbeddedPropertyMapping".equals(Composing.type(pm))) {
            Json.Obj otherwise = pm.getObj("otherwisePropertyMapping");
            String target = str(otherwise, "target");
            b.append(" Otherwise (").append("[").append(target == null ? "" : convertIdentifier(target)).append("]: ")
                    .append(ops.render(otherwise.get("relationalOperation"))).append(")");
        }
        return b.toString();
    }
}
