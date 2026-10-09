// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * A mapping's tests as upstream prints them: the legacy {@code MappingTests} and the {@code testSuites}
 * ({@code HelperMappingGrammarComposer.renderMappingTest}, {@code renderMappingTestSuite} and the relational
 * extension's test input data) -- over the records ({@link Protocol.PLegacyMappingTest},
 * {@link Protocol.PMappingTestSuite}; the protocol program's leg 2, step 3).
 */
final class MappingTestComposer {

    private MappingTestComposer() {
    }

    /** {@code renderMappingTest}. */
    static String legacyTest(Protocol.PLegacyMappingTest test, PureComposer.Style style) {
        List<String> data = new ArrayList<>();
        for (Protocol.PLegacyInputData d : test.inputData()) {
            data.add(tab(4) + inputData(d));
        }
        return "  " + test.name() + "\n"
                + tab(2) + "(\n"
                + tab(3) + "query: " + Composing.valueSpecification(test.query(), style) + ";\n"
                + tab(3) + "data:\n"
                + tab(3) + "[\n"
                + String.join(",\n", data) + (data.isEmpty() ? "" : "\n")
                + tab(3) + "];\n"
                + tab(3) + "assert: " + convertString(test.expectedOutput(), false) + ";\n"
                + tab(2) + ")";
    }

    private static String inputData(Protocol.PLegacyInputData d) {
        if (!d.relational()) {
            return "<Object, " + d.inputType() + ", " + Composing.convertPath(d.targetPath()) + ", "
                    + convertString(d.data(), false) + ">";
        }
        String inputType = d.inputType();
        String raw = d.data();
        String data;
        if ("SQL".equals(inputType)) {
            List<String> lines = new ArrayList<>();
            for (String l : Composing.splitDroppingTrailingEmpties(raw.replace("\r", "").replace("\n", ""), ';', '\\')) {
                lines.add(tab(5) + convertString(l + ";\n", true).replace("\\\\;", "\\;"));
            }
            data = "\n" + String.join("+\n", lines);
        } else if ("CSV".equals(inputType)) {
            List<String> lines = new ArrayList<>(Composing.splitDroppingTrailingEmpties(raw, '\n', (char) 0));
            lines.add("\n\n");
            List<String> out = new ArrayList<>();
            for (String l : lines) {
                out.add(tab(5) + convertString(l + "\n", true));
            }
            data = "\n" + String.join("+\n", out);
        } else {
            data = raw;
        }
        return "<Relational, " + inputType + ", " + d.targetPath() + ", " + data + "\n" + tab(4) + ">";
    }

    /** {@code renderMappingTestSuite}. */
    static String testSuite(Protocol.PMappingTestSuite suite, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(tab(1)).append(suite.id()).append(":\n").append(tab(2)).append("{\n");
        if (suite.doc() != null) {
            b.append(tab(3)).append("doc: ").append(convertString(suite.doc(), true)).append(";\n");
        }
        b.append(tab(3)).append("function: ").append(Composing.valueSpecification(suite.func(), style)).append(";\n");
        if (!suite.tests().isEmpty()) {
            List<String> ts = new ArrayList<>();
            for (Protocol.PMappingTest t : suite.tests()) {
                ts.add(test(t, style));
            }
            b.append(tab(3)).append("tests:\n").append(tab(3)).append("[\n").append(String.join(",\n", ts)).append("\n")
                    .append(tab(3)).append("];\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    /** {@code renderMappingTests}. */
    private static String test(Protocol.PMappingTest test, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(tab(4)).append(test.id()).append(":\n").append(tab(4)).append("{\n");
        if (test.doc() != null) {
            b.append(tab(5)).append("doc: ").append(convertString(test.doc(), true)).append(";\n");
        }
        b.append(storeTestData(test.storeTestData(), 4));
        List<String> asserts = new ArrayList<>();
        for (Protocol.PTestAssertion a : test.assertions()) {
            asserts.add(TestAssertionComposer.compose(a, tab(6), style));
        }
        return b.append(tab(5)).append("asserts:\n").append(tab(5)).append("[\n").append(String.join(",\n", asserts)).append("\n")
                .append(tab(5)).append("];\n").append(tab(4)).append("}").toString();
    }

    /** {@code renderStoreTestData}. */
    private static String storeTestData(List<Protocol.PStoreTestData> data, int base) {
        List<String> out = new ArrayList<>();
        for (Protocol.PStoreTestData d : data) {
            out.add(tab(base + 2) + d.store().path() + ":\n" + EmbeddedDataComposer.compose(data(d), tab(base + 3)));
        }
        return tab(base + 1) + "data:\n" + tab(base + 1) + "[\n"
                + (out.isEmpty() ? "" : String.join(",\n", out) + "\n")
                + tab(base + 1) + "];\n";
    }

    /** The data value a store's test data holds, which the record keeps in one of its forms. */
    private static Protocol.PEmbeddedDataValue data(Protocol.PStoreTestData d) {
        if (d.relationElements() != null) {
            return new Protocol.PRelationData(d.relationElements(), d.relationAccessorSourceInformation());
        }
        if (d.modelData() != null) {
            return new Protocol.PModelStoreData(d.modelData(), d.modelStoreSourceInformation());
        }
        if (d.dataElement() != null) {
            return new Protocol.PDataReference(d.dataElement(), d.dataElement().sourceInformation());
        }
        if (d.embedded() == null) {
            throw Composing.refused("store test data for " + d.store().path() + " that holds no data");
        }
        return d.embedded();
    }
}
