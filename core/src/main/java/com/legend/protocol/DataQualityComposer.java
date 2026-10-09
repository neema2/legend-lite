// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

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
 * element's path as the wire spells it (unquoted) and indents with three spaces per level. A persistence strategy
 * (which upstream's printer has no renderer for) and a tree's sub-type trees have no reader rule: refused when read.
 */
final class DataQualityComposer {

    /** The extension's graph fetch tree layout: the root at three tabs, two spaces each. */
    private static final int INITIAL_TAB_SIZE = 3;

    private DataQualityComposer() {
    }

    /** The section's kinds: data quality validations, relation validations and relation comparisons. */
    // The validation tree prints PRETTY whatever the model's style (upstream's transformer for it is built with
    // RenderStyle.PRETTY); the filter, the query, the assertions, the source and the target follow the model's.
    static String element(Protocol.Element e, PureComposer.Style style) {
        return switch (e) {
            case Protocol.PDataQualityValidation dq -> dataQuality(dq, style);
            case Protocol.PDataQualityRelationValidation v -> relationValidation(v, style);
            case Protocol.PDataQualityRelationComparison c -> relationComparison(c, style);
            default -> throw Composing.refused("the data quality composer has no rule for a " + e.getClass().getSimpleName());
        };
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

    private static String dataQuality(Protocol.PDataQualityValidation dq, PureComposer.Style style) {
        return DomainComposer.declarationPrefix("DataQualityValidation", "", dq.stereotypes(), dq.taggedValues())
                + rawPath(dq.pkg(), dq.name()) + "\n"
                + "{\n"
                + "   context: " + context(dq) + ";\n"
                // upstream's tree transformer: withIndentation(1) on the model's context, then PRETTY -- so one space
                // in a PRETTY model, none in a STANDARD one
                + "   validationTree: " + rootTree(dq.validationTree(), Composing.indented("", 1, style)) + ";\n"
                + (dq.filter() == null ? "" : "   filter: " + valueSpecification(dq.filter(), style) + ";\n")
                + "}";
    }

    /** {@code fromMappingAndRuntime(mapping, runtime)} or {@code fromDataSpace(dataSpace, 'context')}. */
    private static String context(Protocol.PDataQualityValidation dq) {
        if ("fromDataSpace".equals(dq.contextKind())) {
            return "fromDataSpace(" + dq.contextPath() + ", '" + dq.contextSecond() + "')";
        }
        return "fromMappingAndRuntime(" + dq.contextPath() + ", " + dq.contextSecond() + ")";
    }

    /** {@code computeIndentationString(transformer, n)}: the tree transformer's own indentation and {@code n} spaces. */
    private static String spaces(String base, int n) {
        return base + " ".repeat(n);
    }

    private static String rootTree(Protocol.PDqTreeNode root, String base) {
        List<String> subTrees = new ArrayList<>();
        for (Protocol.PDqTreeNode t : root.subTrees()) {
            subTrees.add(propertyTree(t, INITIAL_TAB_SIZE + 1, base));
        }
        String at = spaces(base, 2 * INITIAL_TAB_SIZE);
        return "$[\n" + at + root.className() + constraints(root) + "{\n"
                + String.join(",\n", subTrees) + "\n"
                + at + "}\n"
                + spaces(base, 2 * (INITIAL_TAB_SIZE - 1)) + "]$";
    }

    private static String propertyTree(Protocol.PDqTreeNode tree, int tabSize, String base) {
        String subTreeString = "";
        if (!tree.subTrees().isEmpty()) {
            List<String> out = new ArrayList<>();
            for (Protocol.PDqTreeNode t : tree.subTrees()) {
                out.add(propertyTree(t, tabSize + 1, base));
            }
            subTreeString = "{\n" + String.join(",\n", out) + "\n" + spaces(base, 2 * tabSize) + "}";
        }
        String parameters = "";
        if (!tree.parameters().isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (com.legend.protocol.spec.ValueSpecification p : tree.parameters()) {
                ps.add(PureComposer.valueSpecification(p, PureComposer.Style.PRETTY, base));
            }
            parameters = "(" + String.join(", ", ps) + ")";
        }
        return spaces(base, 2 * tabSize) + (tree.alias() != null ? Composing.convertString(tree.alias(), false) + ":" : "")
                + tree.property() + constraints(tree) + parameters
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

    private static String relationValidation(Protocol.PDataQualityRelationValidation v, PureComposer.Style style) {
        List<String> validations = new ArrayList<>();
        for (Protocol.PDqRelationCheck val : v.validations()) {
            validations.add(validation(val, style));
        }
        return DomainComposer.declarationPrefix("DataQualityRelationValidation", "", v.stereotypes(), v.taggedValues())
                + rawPath(v.pkg(), v.name()) + "\n"
                + "{\n"
                + "   query: " + valueSpecification(v.query(), style) + ";\n"
                + "   validations: [\n" + String.join(",\n", validations) + "\n   ];\n"
                + testSuites(v.testSuites(), style)
                + "}";
    }

    private static String validation(Protocol.PDqRelationCheck val, PureComposer.Style style) {
        return "   {\n"
                + "     name: '" + val.name() + "';\n"
                + (val.description() == null ? "" : "     description: '" + val.description() + "';\n")
                + "     assertion: " + valueSpecification(val.assertion(), style) + ";\n"
                + (val.type() == null ? "" : "     type: " + val.type() + ";\n")
                + "    }";
    }

    // ---------------------------------------------------------------------
    // DataQualityRelationComparison
    // ---------------------------------------------------------------------

    private static String relationComparison(Protocol.PDataQualityRelationComparison c, PureComposer.Style style) {
        return "DataQualityRelationComparison " + rawPath(c.pkg(), c.name()) + "\n"
                + "{\n"
                + "   source: " + valueSpecification(c.source(), style) + ";\n"
                + "   target: " + valueSpecification(c.target(), style) + ";\n"
                + (c.keys().isEmpty() ? "" : "   keys: [" + String.join(", ", c.keys()) + "];\n")
                + (c.columnsToCompare().isEmpty() ? "" : "   columnsToCompare: [" + String.join(", ", c.columnsToCompare()) + "];\n")
                + "   strategy: " + strategy(c.strategy()) + ";\n"
                + (c.expectedMatch() == null ? "" : "   expectedMatch: " + c.expectedMatch() + ";\n")
                + testSuites(c.testSuites(), style)
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

    private static String testSuites(@com.legend.base.Nullable List<Protocol.PDqTestSuite> suites, PureComposer.Style style) {
        if (suites == null || suites.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PDqTestSuite s : suites) {
            out.add(testSuite(s, 2, style));
        }
        return indent(1) + "testSuites:\n" + indent(1) + "[\n" + String.join(",\n", out) + "\n" + indent(1) + "]\n";
    }

    private static String testSuite(Protocol.PDqTestSuite suite, int base, PureComposer.Style style) {
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
                ts.add(test(t, base + 2, style));
            }
            b.append(indent(base + 1)).append("tests:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", ts))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        return b.append(indent(base)).append("}").toString();
    }

    private static String test(Protocol.PDqTest t, int base, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(indent(base)).append(convertIdentifier(t.id())).append(":\n").append(indent(base)).append("{\n");
        if (!t.assertions().isEmpty()) {
            List<String> as = new ArrayList<>();
            for (Protocol.PTestAssertion a : t.assertions()) {
                as.add(TestAssertionComposer.compose(a, indent(base + 2), style));
            }
            b.append(indent(base + 1)).append("asserts:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", as))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        return b.append(indent(base)).append("}").toString();
    }
}
