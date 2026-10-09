// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * A function's test suites, as upstream prints them after its body
 * ({@code HelperDomainGrammarComposer.renderFunctionTestSuites} and its helpers) -- over the records
 * ({@link Protocol.PTestSuite}; the protocol program's leg 2, step 3).
 */
final class FunctionTestComposer {

    private FunctionTestComposer() {
    }

    static String testSuites(Protocol.PFunction function) {
        List<Protocol.PTestSuite> suites = function.testSuites();
        if (suites.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PTestSuite s : suites) {
            out.add(suite(function, s));
        }
        return "\n{\n" + String.join("\n" + (suites.size() > 1 ? "\n" : ""), out) + "\n}";
    }

    /** The unnamed suite (the wire's {@code default}) prints its tests bare; a named one in its own block. */
    private static String suite(Protocol.PFunction function, Protocol.PTestSuite suite) {
        String id = suite.id();
        int level = id != null ? 2 : 1;
        StringBuilder b = new StringBuilder();
        if (id != null) {
            b.append(tab(1)).append(id).append("\n").append(tab(1)).append("(\n");
        }
        List<String> data = new ArrayList<>();
        for (Protocol.PTestData d : suite.testData()) {
            data.add(testData(d, level));
        }
        if (!data.isEmpty()) {
            b.append(String.join("\n", data)).append("\n");
        }
        List<String> tests = new ArrayList<>();
        for (Protocol.PFunctionTest t : suite.tests()) {
            tests.add(test(function, t, level));
        }
        b.append(String.join("\n", tests));
        if (id != null) {
            b.append("\n").append(tab(1)).append(")");
        }
        return b.toString();
    }

    private static String testData(Protocol.PTestData d, int level) {
        StringBuilder b = new StringBuilder(tab(level)).append(RuntimeComposer.elementPointer(d.pointerType(), d.storePath()))
                .append(":");
        switch (d.data()) {
            case Protocol.PTestPayload.Reference r -> b.append(" ").append(r.path());
            case Protocol.PTestPayload.ExternalFormat e -> b.append(" ").append(simpleExternalFormat(e));
            default -> b.append("\n").append(EmbeddedDataComposer.compose(d.data(), tab(level + 2)));
        }
        return b.append(";").toString();
    }

    /** A test calls the function by its declared name. */
    private static String test(Protocol.PFunction function, Protocol.PFunctionTest test, int level) {
        List<String> params = new ArrayList<>();
        for (Protocol.PTestParam p : test.parameters()) {
            params.add(Composing.valueSpecification(p.value()));
        }
        return tab(level) + test.id() + (test.doc() != null ? " " + convertString(test.doc(), true) : "")
                + " | " + function.name() + "(" + String.join(",", params) + ") => "
                + assertion(test.assertion(), level) + ";";
    }

    private static String assertion(Protocol.PAssertion a, int level) {
        return switch (a) {
            case Protocol.PAssertion.EqualTo e -> Composing.valueSpecification(e.expected());
            case Protocol.PAssertion.EqualToJson j -> simpleExternalFormat(j.expected());
            case Protocol.PAssertion.EqualToRelation r -> "Relation\n" + EmbeddedDataComposer.alignedRelation(
                    new Protocol.PRelationElement(r.expected().columns(), r.expected().paths(), r.expected().rows(),
                            r.expected().sourceInformation()), tab(level), true);
        };
    }

    /** {@code renderSimpleExternalFormat}: {@code (JSON) '...'}. */
    private static String simpleExternalFormat(Protocol.PTestPayload.ExternalFormat data) {
        String contentType = data.contentType();
        String label = "application/json".equals(contentType) ? "JSON" : "application/xml".equals(contentType) ? "XML" : contentType;
        return "(" + label + ") " + convertString(data.data(), true);
    }
}
