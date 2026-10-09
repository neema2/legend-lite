// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.CTime;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * Embedded data ({@code Kind #{ ... }#}) as upstream prints it ({@code HelperEmbeddedDataGrammarComposer}
 * with core's and the stores' embedded data composers: external format, reference, model store, relation
 * elements, relational CSV, service store), over the records -- a data element's, a test's or an assertion's value
 * ({@link Protocol.PEmbeddedDataValue}) and a function test's ({@link Protocol.PTestPayload}): one wire, two record
 * families, one printer (the protocol program's leg 2, step 3).
 */
final class EmbeddedDataComposer {

    private static final String PARENTHESES = "()";
    private static final String BRACKETS = "[]";

    private EmbeddedDataComposer() {
    }

    /** {@code composeEmbeddedData} at the context's indentation {@code i}. */
    static String compose(Protocol.PEmbeddedDataValue data, String i) {
        String inner = i + TAB;
        return switch (data) {
            case Protocol.PDataReference r -> block("DATASPACE".equals(r.dataElement().type()) ? "DataspaceTestData"
                    : "Reference", inner + Composing.convertPath(r.dataElement().path()), i);
            case Protocol.PExternalFormatData e -> block("ExternalFormat", externalFormat(e.contentType(), e.data(), inner), i);
            case Protocol.PModelStoreData m -> block("ModelStore", modelStore(m, inner), i);
            case Protocol.PRelationData r -> block("Relation", relationElements(r.relationElements(), inner), i);
            case Protocol.PRelationalCsvData c -> block("Relational", relationalCsv(c, inner), i);
            case Protocol.PServiceStoreData s -> block("ServiceStore", ServiceStoreComposer.embeddedData(s, inner), i);
        };
    }

    /** A function test's data, the same printer over the function-test records. */
    static String compose(Protocol.PTestPayload data, String i) {
        String inner = i + TAB;
        return switch (data) {
            case Protocol.PTestPayload.Reference r -> block("DATASPACE".equals(r.refType()) ? "DataspaceTestData"
                    : "Reference", inner + Composing.convertPath(r.path()), i);
            case Protocol.PTestPayload.ExternalFormat e -> block("ExternalFormat",
                    externalFormat(e.contentType(), e.data(), inner), i);
            case Protocol.PTestPayload.ModelStoreData m -> {
                List<String> out = new ArrayList<>();
                for (Protocol.PTestPayload.ModelEmbedded me : m.modelData()) {
                    out.add(inner + me.model() + ":\n" + compose(me.data(), inner + TAB));
                }
                yield block("ModelStore", String.join(",\n", out), i);
            }
            case Protocol.PTestPayload.RelationElements r -> {
                List<Protocol.PRelationElement> elements = new ArrayList<>();
                for (Protocol.PTestPayload.RelationElement e : r.elements()) {
                    elements.add(new Protocol.PRelationElement(e.columns(), e.paths(), e.rows(), e.sourceInformation()));
                }
                yield block("Relation", relationElements(elements, inner), i);
            }
            case Protocol.PTestPayload.RelationalCsv c -> {
                List<Protocol.PRelationalCsvTable> tables = new ArrayList<>();
                for (Protocol.PTestPayload.CsvTable t : c.tables()) {
                    tables.add(new Protocol.PRelationalCsvTable(t.schema(), t.table(), t.values(), t.sourceInformation()));
                }
                yield block("Relational", relationalCsv(new Protocol.PRelationalCsvData(tables, null), inner), i);
            }
        };
    }

    private static String block(String keyword, String content, String i) {
        return i + keyword + "\n" + i + "#{\n" + content + "\n" + i + "}#";
    }

    private static String externalFormat(String contentType, String data, String i) {
        return i + "contentType: " + convertString(contentType, true) + ";\n"
                + i + "data: " + convertString(data, true) + ";";
    }

    private static String relationalCsv(Protocol.PRelationalCsvData d, String i) {
        List<String> tables = new ArrayList<>();
        for (Protocol.PRelationalCsvTable t : d.tables()) {
            StringBuilder b = new StringBuilder(i).append(t.schema()).append(".").append(t.table()).append(":");
            List<String> lines = new ArrayList<>();
            for (String l : Composing.splitDroppingTrailingEmpties(t.values(), '\n', (char) 0)) {
                lines.add(i + TAB + convertString(l + "\n", true));
            }
            b.append("\n").append(String.join("+\n", lines));
            tables.add(b.append(";").toString());
        }
        return String.join("\n\n", tables);
    }

    // ---------------------------------------------------------------------
    // Relation elements
    // ---------------------------------------------------------------------

    private static String relationElements(List<Protocol.PRelationElement> elements, String i) {
        List<String> out = new ArrayList<>();
        for (Protocol.PRelationElement e : elements) {
            out.add(e.paths().isEmpty() ? alignedRelation(e, i, true)
                    : i + String.join(".", e.paths()) + ":\n" + alignedRelation(e, i + TAB, false));
        }
        return String.join("\n\n", out);
    }

    /** {@code renderAlignedRelationElement}: the columns and rows, padded to their widest value. */
    static String alignedRelation(Protocol.PRelationElement element, String base, boolean standAlone) {
        String inner = base + TAB;
        List<String> columns = element.columns();
        List<List<String>> rows = element.rows();
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

    private static String modelStore(Protocol.PModelStoreData d, String i) {
        List<String> out = new ArrayList<>();
        for (Protocol.PModelData m : d.modelData()) {
            out.add(modelTestData(m, i));
        }
        return String.join(",\n", out);
    }

    private static String modelTestData(Protocol.PModelData data, String i) {
        String indent = i + TAB;
        StringBuilder b = new StringBuilder(i).append(data.model()).append(":\n");
        return switch (data) {
            case Protocol.PModelEmbeddedData e -> b.append(compose(e.data(), indent)).toString();
            case Protocol.PModelInstanceData d -> {
                ValueSpecification vs = d.instances();
                if (vs instanceof PackageableElementPtr ptr) {
                    // a pointer to a data element: printed as that reference
                    yield b.append(compose(new Protocol.PDataReference(new Protocol.PPointer("DATA", ptr.fullPath(), null),
                            null), indent)).toString();
                }
                if (vs instanceof PureCollection c && c.values().size() == 1) {
                    yield b.append(indent).append("[\n").append(indent).append(TAB).append(modelValue(vs, BRACKETS, 2, i))
                            .append("\n").append(indent).append("]").toString();
                }
                yield b.append(indent).append(modelValue(vs, BRACKETS, 1, i)).toString();
            }
        };
    }

    /**
     * One value of model data: upstream's visitor keeps a collection-style stack and an indent level as
     * mutable state; here they are the {@code style} and {@code level} arguments.
     */
    private static String modelValue(ValueSpecification vs, String style, int level, String i) {
        if (vs instanceof PureCollection c) {
            List<ValueSpecification> values = c.values();
            boolean oneLine = values.size() <= 1 || primitive(values.get(0));
            return collection(values, oneLine, style, level, i);
        }
        if (vs instanceof AppliedFunction af && AppliedFunction.NEW.equals(af.function()) && af.parameters().size() == 2
                && af.parameters().get(1) instanceof NewInstance ni) {
            // ^X(k = v, ...): the keys a parenthesized collection, each value as it rides
            List<String> keys = new ArrayList<>();
            for (NewInstance.KeyBinding k : ni.properties()) {
                keys.add(k.key() + " = " + modelValue(k.expression().value(), BRACKETS, level + 1, i));
            }
            return "^" + ni.className() + strings(keys, ni.properties().size() <= 1, PARENTHESES, level, i);
        }
        return modelLiteral(vs);
    }

    /** The literals upstream's model-data printer counts primitive. */
    private static boolean primitive(ValueSpecification v) {
        return v instanceof CString || v instanceof CBoolean || v instanceof CInteger || v instanceof CFloat
                || v instanceof CDecimal || v instanceof CDate || v instanceof CTime || v instanceof CLatestDate;
    }

    private static String modelLiteral(ValueSpecification vs) {
        return switch (vs) {
            case CString s -> convertString(s.value(), true);
            case CDate d -> percent(written(d.written()));
            case CTime t -> percent(written(t.written()));
            case CBoolean b -> String.valueOf(b.value());
            case EnumValue e -> Composing.convertPath(e.fullPath()) + "." + Composing.convertIdentifier(e.value());
            case CInteger n -> n.value().toString();
            case CFloat f -> Double.toString(f.value());
            case CDecimal d -> d.value().toPlainString() + "D";
            default -> throw Composing.refused("no model data rule for a value " + vs.getClass().getSimpleName());
        };
    }

    private static String written(@com.legend.base.Nullable String written) {
        if (written == null) {
            throw Composing.refused("a date in model data without its written form");
        }
        return written;
    }

    private static String percent(String d) {
        return d.indexOf('%') != -1 ? d : "%" + d;
    }

    /** {@code formatCollection}. */
    private static String collection(List<ValueSpecification> values, boolean oneLine, String style, int level, String i) {
        if (values.isEmpty()) {
            return style;
        }
        if (values.size() == 1 && BRACKETS.equals(style)) {
            return modelValue(values.get(0), BRACKETS, level, i);
        }
        List<String> out = new ArrayList<>();
        for (ValueSpecification v : values) {
            out.add(modelValue(v, BRACKETS, level + 1, i));
        }
        return strings(out, oneLine, style, level, i);
    }

    /** {@code formatCollection} over already printed items. */
    private static String strings(List<String> out, boolean oneLine, String style, int level, String i) {
        if (out.isEmpty()) {
            return style;
        }
        String newline = "\n" + i + tab(level + 1);
        return style.charAt(0) + (oneLine ? "" : newline) + String.join(oneLine ? ", " : "," + newline, out)
                + (oneLine ? "" : "\n" + i + tab(level)) + style.charAt(1);
    }
}
