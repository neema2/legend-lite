// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.Protocol.PServiceTestSuite;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * A service's tests as upstream prints them ({@code HelperServiceGrammarComposer}): its test suites, in
 * the block form or the flat form, and the legacy {@code test:} -- over the records ({@link PServiceTestSuite},
 * {@link Protocol.PLegacyServiceTest}; the protocol program's leg 2, step 3).
 */
final class ServiceTestComposer {

    private ServiceTestComposer() {
    }

    /** {@code renderServiceTestSuite}: the flat form when the suite carries service test data. */
    static String testSuite(PServiceTestSuite suite, PureComposer.Style style) {
        PServiceTestSuite.PSuiteData testData = suite.testData();
        List<PServiceTestSuite.PResolverData> resolvers = testData == null ? null : testData.serviceTestData();
        if (resolvers != null && !resolvers.isEmpty()) {
            return flatSuite(suite, resolvers, style);
        }
        return blockSuite(suite, testData, style);
    }

    private static String blockSuite(PServiceTestSuite suite, PServiceTestSuite.@com.legend.base.Nullable PSuiteData testData,
            PureComposer.Style style) {
        StringBuilder b = new StringBuilder(tab(2)).append(convertIdentifier(suite.id())).append(":\n").append(tab(2)).append("{\n");
        if (testData != null) {
            b.append(tab(3)).append("data:\n").append(tab(3)).append("[\n");
            List<PServiceTestSuite.PSuiteConnData> connections = testData.connectionsTestData();
            if (!connections.isEmpty()) {
                List<String> cs = new ArrayList<>();
                for (PServiceTestSuite.PSuiteConnData c : connections) {
                    cs.add(tab(5) + c.id() + ":\n" + EmbeddedDataComposer.compose(c.data(), tab(6)));
                }
                b.append(tab(4)).append("connections:\n").append(tab(4)).append("[\n").append(String.join(",\n", cs)).append("\n")
                        .append(tab(4)).append("]\n");
            }
            b.append(tab(3)).append("]\n");
        }
        List<String> ts = new ArrayList<>();
        for (PServiceTestSuite.PSuiteTest t : suite.tests()) {
            ts.add(blockTest(t, 4, style));
        }
        b.append(tab(3)).append("tests:\n").append(tab(3)).append("[\n").append(String.join(",\n", ts)).append("\n").append(tab(3)).append("]\n");
        return b.append(tab(2)).append("}").toString();
    }

    private static String blockTest(PServiceTestSuite.PSuiteTest test, int base, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(tab(base)).append(convertIdentifier(test.id())).append(":\n").append(tab(base)).append("{\n");
        if (test.serializationFormat() != null) {
            b.append(tab(base + 1)).append("serializationFormat: ").append(test.serializationFormat()).append(";\n");
        }
        List<PServiceTestSuite.PSuiteParam> params = test.parameters() == null ? List.of() : test.parameters();
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (PServiceTestSuite.PSuiteParam p : params) {
                ps.add(tab(base + 2) + p.name() + " = " + valueSpecification(p.value(), style));
            }
            b.append(tab(base + 1)).append("parameters:\n").append(tab(base + 1)).append("[\n").append(String.join(",\n", ps)).append("\n")
                    .append(tab(base + 1)).append("]\n");
        }
        List<String> keys = keys(test);
        if (!keys.isEmpty()) {
            b.append(tab(base + 1)).append("keys:\n").append(tab(base + 1)).append("[\n").append(tab(base + 2)).append(String.join(",\n", keys))
                    .append("\n").append(tab(base + 1)).append("];\n");
        }
        List<String> as = new ArrayList<>();
        for (Protocol.PTestAssertion a : test.assertions()) {
            as.add(TestAssertionComposer.compose(a, tab(base + 2), style));
        }
        b.append(tab(base + 1)).append("asserts:\n").append(tab(base + 1)).append("[\n").append(String.join(",\n", as)).append("\n")
                .append(tab(base + 1)).append("]\n");
        return b.append(tab(base)).append("}").toString();
    }

    private static List<String> keys(PServiceTestSuite.PSuiteTest test) {
        List<String> out = new ArrayList<>();
        for (String k : test.keys()) {
            out.add(convertString(k, true));
        }
        return out;
    }

    private static String flatSuite(PServiceTestSuite suite, List<PServiceTestSuite.PResolverData> resolvers,
            PureComposer.Style style) {
        StringBuilder b = new StringBuilder(tab(2)).append(convertIdentifier(suite.id()))
                .append(suite.doc() != null ? " " + convertString(suite.doc(), true) : "").append("\n").append(tab(2)).append("(\n");
        for (PServiceTestSuite.PResolverData r : resolvers) {
            b.append(resolver(r, 3)).append("\n");
        }
        for (PServiceTestSuite.PSuiteTest t : suite.tests()) {
            b.append(atomicTest(t, 3, style)).append("\n");
        }
        return b.append(tab(2)).append(")").toString();
    }

    /** A reference resolver ({@code path;}) carries no data; a base resolver its data block. */
    private static String resolver(PServiceTestSuite.PResolverData r, int base) {
        Protocol.PEmbeddedDataValue data = r.data();
        return data == null ? tab(base) + r.elementPath() + ";"
                : tab(base) + r.elementPath() + ":\n" + EmbeddedDataComposer.compose(data, tab(base + 1)) + ";";
    }

    private static String atomicTest(PServiceTestSuite.PSuiteTest test, int base, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(tab(base)).append(convertIdentifier(test.id()));
        if (test.doc() != null) {
            b.append(" ").append(convertString(test.doc(), true));
        }
        List<PServiceTestSuite.PSuiteParam> params = test.parameters() == null ? List.of() : test.parameters();
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (PServiceTestSuite.PSuiteParam p : params) {
                ps.add(p.name() + " = " + valueSpecification(p.value(), style));
            }
            b.append(" (").append(String.join(", ", ps)).append(")");
        }
        List<String> keys = keys(test);
        if (!keys.isEmpty()) {
            b.append(" [").append(String.join(", ", keys)).append("]");
        }
        if (test.serializationFormat() != null) {
            b.append(" : ").append(test.serializationFormat());
        }
        b.append(" =>\n");
        if (test.assertions().size() != 1) {
            throw Composing.refused("a flat service test with " + test.assertions().size() + " assertions (upstream cannot print it)");
        }
        switch (test.assertions().get(0).expected()) {
            case Protocol.PRelationElement r ->
                    b.append(tab(base + 1)).append("Relation\n").append(EmbeddedDataComposer.alignedRelation(r, tab(base + 1), true));
            case Protocol.PExternalFormatData e -> b.append(EmbeddedDataComposer.compose(e, tab(base + 1)));
            case Protocol.PEqualToValue v ->
                    throw Composing.refused("a flat service test asserting by _type 'equalTo' (upstream cannot print it)");
        }
        return b.append(";").toString();
    }

    // ---------------------------------------------------------------------
    // The legacy test
    // ---------------------------------------------------------------------

    /** {@code isServiceTestEmpty}. */
    static boolean legacyTestEmpty(Protocol.PLegacyServiceTest test) {
        if ("Single".equals(test.kind())) {
            return test.asserts().isEmpty();
        }
        for (Protocol.PLegacyServiceTest.PKeyedLegacyTest t : test.keyedTests()) {
            if (!t.asserts().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    static String legacyTest(Protocol.PLegacyServiceTest test, PureComposer.Style style) {
        if ("Single".equals(test.kind())) {
            if (test.data() == null) {
                throw Composing.refused("a legacy single-execution service test without its data");
            }
            return "Single\n" + TAB + "{\n"
                    + tab(2) + "data: " + convertString(test.data(), true) + ";\n"
                    + tab(2) + "asserts:\n" + testContainers(test.asserts(), 2, style) + "\n"
                    + TAB + "}\n";
        }
        List<String> tests = new ArrayList<>();
        for (Protocol.PLegacyServiceTest.PKeyedLegacyTest t : test.keyedTests()) {
            tests.add(tab(2) + "tests[" + convertString(t.key(), true) + "]:\n" + tab(2) + "{\n"
                    + tab(3) + "data: " + convertString(t.data(), true) + ";\n"
                    + tab(3) + "asserts:\n" + testContainers(t.asserts(), 3, style)
                    + "\n" + tab(2) + "}");
        }
        return "Multi\n" + TAB + "{\n" + String.join("\n", tests) + "\n" + TAB + "}\n";
    }

    private static String testContainers(List<Protocol.PLegacyServiceTest.PLegacyAssert> containers, int indent,
            PureComposer.Style style) {
        List<String> out = new ArrayList<>();
        for (Protocol.PLegacyServiceTest.PLegacyAssert c : containers) {
            List<String> params = new ArrayList<>();
            for (ValueSpecification p : c.parametersValues()) {
                params.add(PureComposer.legacyServiceParameter(p, style));
            }
            out.add(tab(indent + 1) + "{ [" + String.join(", ", params) + "], " + valueSpecification(c.assertion(), style) + " }");
        }
        return tab(indent) + "[\n" + String.join(",\n", out) + (out.isEmpty() ? "" : "\n") + tab(indent) + "];";
    }
}
