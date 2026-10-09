// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * {@code ###DataQualityValidation}'s elements as upstream prints them ({@code DataQualityGrammarComposerExtension}):
 * the data quality validation (its constraint tree printed in the extension's own pretty layout), the relation
 * validation and the relation comparison, with their test suites -- over the records
 * ({@link Protocol.PDataQualityValidation}, {@link Protocol.PDataQualityRelationValidation},
 * {@link Protocol.PDataQualityRelationComparison}; the protocol program's leg 2, step 3). The extension writes the
 * element's path as the wire spells it (unquoted) and indents with three spaces per level. What its printer has no
 * renderer for (a persistence strategy, a tree's sub-type trees or parameters) has no reader rule: refused when read.
 */
final class DataQualityComposer {

    /** The extension's graph fetch tree layout: the root at three tabs, two spaces each. */
    private static final int INITIAL_TAB_SIZE = 3;

    private DataQualityComposer() {
    }

    /** The section's kinds: data quality validations, relation validations and relation comparisons. */
    static String element(Protocol.Element e) {
        return switch (e) {
            case Protocol.PDataQualityValidation dq -> dataQuality(dq);
            case Protocol.PDataQualityRelationValidation v -> relationValidation(v);
            case Protocol.PDataQualityRelationComparison c -> relationComparison(c);
            default -> throw Composing.refused("the data quality composer has no rule for a " + e.getClass().getSimpleName());
        };
    }

    /** {@link #element(Protocol.Element)} of the JSON, read first. */
    static String element(Json.Obj e) {
        return element(Composing.element(e, Protocol.Element.class));
    }

    /** The element's path as the extension writes it: package, {@code ::}, name, no quoting. */
    private static String rawPath(String pkg, String name) {
        return pkg.isEmpty() ? name : pkg + "::" + name;
    }

    private static String indent(int level) {
        return "   ".repeat(level);
    }

    // ---------------------------------------------------------------------
    // DataQualityValidation
    // ---------------------------------------------------------------------

    private static String dataQuality(Protocol.PDataQualityValidation dq) {
        return DomainComposer.declarationPrefix("DataQualityValidation", "", dq.stereotypes(), dq.taggedValues())
                + rawPath(dq.pkg(), dq.name()) + "\n"
                + "{\n"
                + "   context: " + context(dq) + ";\n"
                + "   validationTree: " + rootTree(dq.validationTree()) + ";\n"
                + (dq.filter() == null ? "" : "   filter: " + valueSpecification(dq.filter()) + ";\n")
                + "}";
    }

    /** {@code fromMappingAndRuntime(mapping, runtime)} or {@code fromDataSpace(dataSpace, 'context')}. */
    private static String context(Protocol.PDataQualityValidation dq) {
        if ("fromDataSpace".equals(dq.contextKind())) {
            return "fromDataSpace(" + dq.contextPath() + ", '" + dq.contextSecond() + "')";
        }
        return "fromMappingAndRuntime(" + dq.contextPath() + ", " + dq.contextSecond() + ")";
    }

    /** Pretty indentation: {@code n} spaces (the extension's transformer has no base indentation). */
    private static String spaces(int n) {
        return " ".repeat(n);
    }

    private static String rootTree(Protocol.PDqTreeNode root) {
        List<String> subTrees = new ArrayList<>();
        for (Protocol.PDqTreeNode t : root.subTrees()) {
            subTrees.add(propertyTree(t, INITIAL_TAB_SIZE + 1));
        }
        String at = spaces(2 * INITIAL_TAB_SIZE);
        return "$[\n" + at + root.className() + constraints(root) + "{\n"
                + String.join(",\n", subTrees) + "\n"
                + at + "}\n"
                + spaces(2 * (INITIAL_TAB_SIZE - 1)) + "]$";
    }

    private static String propertyTree(Protocol.PDqTreeNode tree, int tabSize) {
        String subTreeString = "";
        if (!tree.subTrees().isEmpty()) {
            List<String> out = new ArrayList<>();
            for (Protocol.PDqTreeNode t : tree.subTrees()) {
                out.add(propertyTree(t, tabSize + 1));
            }
            subTreeString = "{\n" + String.join(",\n", out) + "\n" + spaces(2 * tabSize) + "}";
        }
        return spaces(2 * tabSize) + tree.property() + constraints(tree)
                + (tree.subType() != null ? "->subType(@" + tree.subType() + ")" : "") + subTreeString;
    }

    private static String constraints(Protocol.PDqTreeNode tree) {
        if (tree.constraints().isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (String c : tree.constraints()) {
            out.add(convertIdentifier(c));
        }
        return "<" + String.join(", ", out) + ">";
    }

    // ---------------------------------------------------------------------
    // DataQualityRelationValidation
    // ---------------------------------------------------------------------

    private static String relationValidation(Protocol.PDataQualityRelationValidation v) {
        List<String> validations = new ArrayList<>();
        for (Protocol.PDqRelationCheck val : v.validations()) {
            validations.add(validation(val));
        }
        return DomainComposer.declarationPrefix("DataQualityRelationValidation", "", v.stereotypes(), v.taggedValues())
                + rawPath(v.pkg(), v.name()) + "\n"
                + "{\n"
                + "   query: " + valueSpecification(v.query()) + ";\n"
                + "   validations: [\n" + String.join(",\n", validations) + "\n   ];\n"
                + testSuites(v.testSuites())
                + "}";
    }

    private static String validation(Protocol.PDqRelationCheck val) {
        return "   {\n"
                + "     name: '" + val.name() + "';\n"
                + (val.description() == null ? "" : "     description: '" + val.description() + "';\n")
                + "     assertion: " + valueSpecification(val.assertion()) + ";\n"
                + (val.type() == null ? "" : "     type: " + val.type() + ";\n")
                + "    }";
    }

    // ---------------------------------------------------------------------
    // DataQualityRelationComparison
    // ---------------------------------------------------------------------

    private static String relationComparison(Protocol.PDataQualityRelationComparison c) {
        return "DataQualityRelationComparison " + rawPath(c.pkg(), c.name()) + "\n"
                + "{\n"
                + "   source: " + valueSpecification(c.source()) + ";\n"
                + "   target: " + valueSpecification(c.target()) + ";\n"
                + (c.keys().isEmpty() ? "" : "   keys: [" + String.join(", ", c.keys()) + "];\n")
                + (c.columnsToCompare().isEmpty() ? "" : "   columnsToCompare: [" + String.join(", ", c.columnsToCompare()) + "];\n")
                + "   strategy: " + strategy(c.strategy()) + ";\n"
                + (c.expectedMatch() == null ? "" : "   expectedMatch: " + c.expectedMatch() + ";\n")
                + testSuites(c.testSuites())
                + "}";
    }

    private static String strategy(Protocol.PReconStrategy s) {
        List<String> fields = new ArrayList<>();
        if (s.sourceHashColumn() != null) {
            fields.add("     sourceHashColumn: " + s.sourceHashColumn() + ";");
        }
        if (s.targetHashColumn() != null) {
            fields.add("     targetHashColumn: " + s.targetHashColumn() + ";");
        }
        if (s.aggregatedHash() != null) {
            fields.add("     aggregatedHash: " + s.aggregatedHash() + ";");
        }
        return fields.isEmpty() ? "MD5Hash" : "MD5Hash\n   {\n" + String.join("\n", fields) + "\n   }";
    }

    // ---------------------------------------------------------------------
    // Test suites
    // ---------------------------------------------------------------------

    private static String testSuites(@com.legend.base.Nullable List<Protocol.PDqTestSuite> suites) {
        if (suites == null || suites.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PDqTestSuite s : suites) {
            out.add(testSuite(s, 2));
        }
        return indent(1) + "testSuites:\n" + indent(1) + "[\n" + String.join(",\n", out) + "\n" + indent(1) + "]\n";
    }

    private static String testSuite(Protocol.PDqTestSuite suite, int base) {
        StringBuilder b = new StringBuilder(indent(base)).append(convertIdentifier(suite.id())).append(":\n").append(indent(base)).append("{\n");
        List<Protocol.PDqStoreData> testData = suite.testData() == null ? List.of() : suite.testData().testData();
        if (!testData.isEmpty()) {
            List<String> data = new ArrayList<>();
            for (Protocol.PDqStoreData td : testData) {
                data.add(indent(base + 2) + td.store() + ":\n" + EmbeddedDataComposer.compose(td.data(), indent(base + 3)));
            }
            b.append(indent(base + 1)).append("data:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", data))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        if (!suite.tests().isEmpty()) {
            List<String> ts = new ArrayList<>();
            for (Protocol.PDqTest t : suite.tests()) {
                ts.add(test(t, base + 2));
            }
            b.append(indent(base + 1)).append("tests:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", ts))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        return b.append(indent(base)).append("}").toString();
    }

    private static String test(Protocol.PDqTest t, int base) {
        StringBuilder b = new StringBuilder(indent(base)).append(convertIdentifier(t.id())).append(":\n").append(indent(base)).append("{\n");
        if (!t.assertions().isEmpty()) {
            List<String> as = new ArrayList<>();
            for (Protocol.PTestAssertion a : t.assertions()) {
                as.add(TestAssertionComposer.compose(a, indent(base + 2)));
            }
            b.append(indent(base + 1)).append("asserts:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", as))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        return b.append(indent(base)).append("}").toString();
    }
}
