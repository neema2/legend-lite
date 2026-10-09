// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.tab;

/**
 * A relational class mapping and association mapping as upstream prints them
 * ({@code RelationalGrammarComposerExtension}'s class and association mapping composers and
 * {@code HelperRelationalGrammarComposer}'s property mappings) -- over the records ({@link Protocol.PClassMappingRel},
 * {@link Protocol.PRelAssociationMapping}; the protocol program's leg 2, step 3).
 */
final class RelationalMappingComposer {

    private RelationalMappingComposer() {
    }

    static String classMapping(Protocol.PClassMappingRel cm, PureComposer.Style style) {
        RelationalOperations ops = RelationalOperations.mapping("", style);
        StringBuilder b = new StringBuilder(": Relational\n").append(TAB).append("{\n");
        Protocol.PFilterMapping filter = cm.filter();
        if (filter != null) {
            b.append(tab(2)).append(RelationalOperations.filterMapping(filter.db(), filter.name(), filter.joins()))
                    .append("\n");
        }
        if (cm.distinct()) {
            b.append(tab(2)).append("~distinct\n");
        }
        operations(b, "~groupBy", cm.groupBy(), ops);
        operations(b, "~primaryKey", cm.primaryKey(), ops);
        Protocol.PTablePtr mainTable = cm.mainTable();
        if (mainTable != null) {
            String schema = mainTable.schema();
            String table = mainTable.table();
            b.append(tab(2)).append("~mainTable [").append(RelationalOperations.tableDb(mainTable)).append("]")
                    .append(!"default".equals(schema) ? schema + "." + table : table).append("\n");
        }
        if (!cm.propertyMappings().isEmpty()) {
            b.append(propertyMappings(cm.propertyMappings(), ops.indented(4))).append("\n");
        }
        return b.append(TAB).append("}").toString();
    }

    private static void operations(StringBuilder b, String keyword, List<Protocol.PRelOp> operations,
            RelationalOperations ops) {
        if (operations.isEmpty()) {
            return;
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PRelOp op : operations) {
            out.add(tab(3) + ops.render(op));
        }
        b.append(tab(2)).append(keyword).append("\n").append(tab(2)).append("(\n")
                .append(String.join(",\n", out)).append("\n").append(tab(2)).append(")\n");
    }

    static String associationMapping(Protocol.PRelAssociationMapping am, String association, PureComposer.Style style) {
        RelationalOperations ops = RelationalOperations.mapping("", style).indented(6);
        List<String> lines = new ArrayList<>();
        for (Protocol.PRelAssocPropertyMapping pm : am.propertyMappings()) {
            // an association's side prints its source set id: renderSourceId
            String source = pm.source();
            lines.add(ops.indentation() + convertIdentifier(pm.property())
                    + target(((source == null || source.isEmpty()) ? "" : source + ","), pm.target()) + ": "
                    + ops.render(pm.relationalOperation()));
        }
        return association + ": Relational\n" + TAB + "{\n"
                + tab(2) + "AssociationMapping\n" + tab(2) + "(\n"
                + (lines.isEmpty() ? "" : String.join(",\n", lines) + "\n")
                + tab(2) + ")\n" + TAB + "}";
    }

    /** {@code [source,target]} after the property, or nothing when there is no target. */
    private static String target(String sourcePrefix, @com.legend.base.Nullable String target) {
        return target == null || target.isEmpty() ? "" : "[" + sourcePrefix + target + "]";
    }

    private static String propertyMappings(List<Protocol.PPropertyMapping> pms, RelationalOperations ops) {
        List<String> out = new ArrayList<>();
        for (Protocol.PPropertyMapping pm : pms) {
            out.add(propertyMapping(pm, ops));
        }
        return String.join(",\n", out);
    }

    /** {@code renderAbstractRelationalPropertyMapping}, in a class mapping (no source set id). */
    private static String propertyMapping(Protocol.PPropertyMapping pm, RelationalOperations ops) {
        return switch (pm) {
            case Protocol.PRelPropertyMapping r -> relationalPropertyMapping(r, ops);
            case Protocol.PEmbeddedPropertyMapping e -> embedded(e.property(), e.propertyMappings(), ops).toString();
            case Protocol.POtherwiseEmbeddedPropertyMapping o -> embedded(o.property(), o.propertyMappings(), ops)
                    .append(" Otherwise (").append("[").append(convertIdentifier(o.otherwiseTarget())).append("]: ")
                    .append(ops.render(o.otherwiseOp())).append(")").toString();
            case Protocol.PInlineEmbeddedPropertyMapping i -> ops.indentation() + convertIdentifier(i.property())
                    + "() Inline[" + convertIdentifier(i.setImplementationId()) + "]";
        };
    }

    private static String relationalPropertyMapping(Protocol.PRelPropertyMapping pm, RelationalOperations ops) {
        Protocol.PLocalProp local = pm.localMappingProperty();
        String property = convertIdentifier(pm.property());
        String head = local != null
                ? "+" + property + ": " + local.type() + "["
                        + Composing.multiplicity(local.lowerBound(), local.upperBound()) + "]"
                : property + target("", pm.target());
        String enumMapping = pm.enumMappingId();
        String binding = pm.bindingTransformer();
        return ops.indentation() + head + ": "
                + (enumMapping != null ? "EnumerationMapping " + convertIdentifier(enumMapping) + ": " : "")
                + (enumMapping == null && binding != null ? "Binding " + Composing.convertPath(binding) + " : " : "")
                + ops.render(pm.relationalOperation());
    }

    /** {@code renderEmbeddedRelationalPropertyMapping}: the property, then its nested lines in parentheses. */
    private static StringBuilder embedded(String property, List<Protocol.PPropertyMapping> nested,
            RelationalOperations ops) {
        StringBuilder b = new StringBuilder(ops.indentation()).append(convertIdentifier(property)).append("\n")
                .append(ops.indentation()).append("(\n");
        if (!nested.isEmpty()) {
            b.append(propertyMappings(nested, ops.indented(2))).append("\n");
        }
        return b.append(ops.indentation()).append(")");
    }
}
