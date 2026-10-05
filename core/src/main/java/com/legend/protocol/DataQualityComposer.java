// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * {@code ###DataQualityValidation}'s elements as upstream prints them ({@code DataQualityGrammarComposerExtension}):
 * the data quality validation (its constraint tree printed in the extension's own pretty layout), the relation
 * validation and the relation comparison, with their test suites. The extension writes the element's path as
 * the wire spells it (unquoted) and indents with three spaces per level.
 */
final class DataQualityComposer {

    /** The extension's graph fetch tree layout: the root at three tabs, two spaces each. */
    private static final int INITIAL_TAB_SIZE = 3;

    private static final Map<String, Function<Json.Obj, String>> PRINTERS = Map.of(
            "dataQualityValidation", DataQualityComposer::dataQuality,
            "dataqualityRelationValidation", DataQualityComposer::relationValidation,
            "dataQualityRelationComparison", DataQualityComposer::relationComparison);

    private DataQualityComposer() {
    }

    /** The section's kinds: data quality validations, relation validations and relation comparisons. */
    static String element(Json.Obj e) {
        Function<Json.Obj, String> printer = PRINTERS.get(Composing.type(e));
        if (printer == null) {
            throw Composing.refused("the data quality composer has no rule for _type '" + Composing.type(e) + "'");
        }
        return printer.apply(e);
    }

    /** The element's path as the extension writes it: package, {@code ::}, name, no quoting. */
    private static String rawPath(Json.Obj e) {
        return Composing.path(e);
    }

    private static String indent(int level) {
        return "   ".repeat(level);
    }

    // ---------------------------------------------------------------------
    // DataQualityValidation
    // ---------------------------------------------------------------------

    private static String dataQuality(Json.Obj dq) {
        Json.Obj filter = objOr(dq, "filter");
        return DomainComposer.declarationPrefix("DataQualityValidation", "", dq) + rawPath(dq) + "\n"
                + "{\n"
                + "   context: " + context(dq.getObj("context")) + ";\n"
                + "   validationTree: " + rootTree(dq.getObj("dataQualityRootGraphFetchTree")) + ";\n"
                + (filter == null ? "" : "   filter: " + valueSpecification(filter) + ";\n")
                + "}";
    }

    private static String context(Json.Obj c) {
        String type = Composing.type(c);
        if ("mappingAndRuntimeDataQualityExecutionContext".equals(type)) {
            return "fromMappingAndRuntime(" + c.getObj("mapping").getString("path") + ", " + c.getObj("runtime").getString("path") + ")";
        }
        if ("dataSpaceDataQualityExecutionContext".equals(type)) {
            return "fromDataSpace(" + c.getObj("dataSpace").getString("path") + ", '" + c.getString("context") + "')";
        }
        throw Composing.refused("no composer rule for a data quality execution context of _type '" + type + "'");
    }

    /** Pretty indentation: {@code n} spaces (the extension's transformer has no base indentation). */
    private static String spaces(int n) {
        return " ".repeat(n);
    }

    private static String rootTree(Json.Obj root) {
        if (!items(root, "subTypeTrees").isEmpty()) {
            throw Composing.refused("a dataQualityRootGraphFetchTree with sub-type trees (no composer rule yet)");
        }
        List<String> subTrees = new ArrayList<>();
        for (Json.Obj t : objs(root, "subTrees")) {
            subTrees.add(propertyTree(t, INITIAL_TAB_SIZE + 1));
        }
        String at = spaces(2 * INITIAL_TAB_SIZE);
        return "$[\n" + at + root.getString("class") + constraints(root) + "{\n"
                + String.join(",\n", subTrees) + "\n"
                + at + "}\n"
                + spaces(2 * (INITIAL_TAB_SIZE - 1)) + "]$";
    }

    private static String propertyTree(Json.Obj tree, int tabSize) {
        if (!"dataQualityPropertyGraphFetchTree".equals(Composing.type(tree))) {
            throw Composing.refused("no composer rule for a data quality tree node of _type '" + Composing.type(tree) + "'");
        }
        String alias = str(tree, "alias");
        String subTreeString = "";
        List<Json.Obj> subTrees = objs(tree, "subTrees");
        if (!subTrees.isEmpty()) {
            List<String> out = new ArrayList<>();
            for (Json.Obj t : subTrees) {
                out.add(propertyTree(t, tabSize + 1));
            }
            subTreeString = "{\n" + String.join(",\n", out) + "\n" + spaces(2 * tabSize) + "}";
        }
        List<Json.Node> params = items(tree, "parameters");
        String parameters = "";
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Node p : params) {
                ps.add(PureComposer.valueSpecification(p, PureComposer.Style.PRETTY, ""));
            }
            parameters = "(" + String.join(", ", ps) + ")";
        }
        String subType = str(tree, "subType");
        return spaces(2 * tabSize) + (alias != null ? convertString(alias, false) + ":" : "") + tree.getString("property")
                + constraints(tree) + parameters + (subType != null ? "->subType(@" + subType + ")" : "") + subTreeString;
    }

    private static String constraints(Json.Obj tree) {
        List<String> constraints = tree.getStringArrayOr("constraints", List.of());
        if (constraints.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (String c : constraints) {
            out.add(convertIdentifier(c));
        }
        return "<" + String.join(", ", out) + ">";
    }

    // ---------------------------------------------------------------------
    // DataQualityRelationValidation
    // ---------------------------------------------------------------------

    private static String relationValidation(Json.Obj v) {
        List<String> validations = new ArrayList<>();
        for (Json.Obj val : objs(v, "validations")) {
            validations.add(validation(val));
        }
        return DomainComposer.declarationPrefix("DataQualityRelationValidation", "", v) + rawPath(v) + "\n"
                + "{\n"
                + "   query: " + valueSpecification(v.get("query")) + ";\n"
                + "   validations: [\n" + String.join(",\n", validations) + "\n   ];\n"
                + persistenceStrategy(v)
                + testSuites(v)
                + "}";
    }

    private static String validation(Json.Obj val) {
        String description = str(val, "description");
        Json.Obj assertion = objOr(val, "assertion");
        String type = str(val, "type");
        return "   {\n"
                + "     name: '" + val.getString("name") + "';\n"
                + (description == null ? "" : "     description: '" + description + "';\n")
                + (assertion == null ? "" : "     assertion: " + valueSpecification(assertion) + ";\n")
                + (type == null ? "" : "     type: " + type + ";\n")
                + "    }";
    }

    /** Upstream registers no persistence strategy renderer: a strategy makes it throw. */
    private static String persistenceStrategy(Json.Obj e) {
        if (Composing.value(e, "persistenceStrategy") != null) {
            throw Composing.refused("a data quality persistence strategy on _type '" + Composing.type(e)
                    + "' (upstream has no renderer for any)");
        }
        return "";
    }

    // ---------------------------------------------------------------------
    // DataQualityRelationComparison
    // ---------------------------------------------------------------------

    private static String relationComparison(Json.Obj c) {
        List<String> keys = c.getStringArrayOr("keys", List.of());
        List<String> columns = c.getStringArrayOr("columnsToCompare", List.of());
        Json.Node expectedMatch = Composing.value(c, "expectedMatch");
        return "DataQualityRelationComparison " + rawPath(c) + "\n"
                + "{\n"
                + "   source: " + valueSpecification(c.get("source")) + ";\n"
                + "   target: " + valueSpecification(c.get("target")) + ";\n"
                + (keys.isEmpty() ? "" : "   keys: [" + String.join(", ", keys) + "];\n")
                + (columns.isEmpty() ? "" : "   columnsToCompare: [" + String.join(", ", columns) + "];\n")
                + "   strategy: " + strategy(c.getObj("strategy")) + ";\n"
                + (expectedMatch == null ? "" : "   expectedMatch: " + RelationalConnectionComposer.raw(expectedMatch) + ";\n")
                + persistenceStrategy(c)
                + testSuites(c)
                + "}";
    }

    private static String strategy(Json.Obj s) {
        if (!"md5Hash".equals(Composing.type(s))) {
            throw Composing.refused("no composer rule for a recon strategy of _type '" + Composing.type(s) + "' (upstream throws)");
        }
        List<String> fields = new ArrayList<>();
        String source = str(s, "sourceHashColumn");
        if (source != null) {
            fields.add("     sourceHashColumn: " + source + ";");
        }
        String target = str(s, "targetHashColumn");
        if (target != null) {
            fields.add("     targetHashColumn: " + target + ";");
        }
        Json.Node aggregated = Composing.value(s, "aggregatedHash");
        if (aggregated != null) {
            fields.add("     aggregatedHash: " + RelationalConnectionComposer.raw(aggregated) + ";");
        }
        return fields.isEmpty() ? "MD5Hash" : "MD5Hash\n   {\n" + String.join("\n", fields) + "\n   }";
    }

    // ---------------------------------------------------------------------
    // Test suites
    // ---------------------------------------------------------------------

    private static String testSuites(Json.Obj e) {
        List<Json.Obj> suites = objs(e, "testSuites");
        if (suites.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Json.Obj s : suites) {
            Json.Obj testData = objOr(s, "testData");
            out.add(testSuite(s.getString("id"), testData == null ? List.of() : objs(testData, "testData"), objs(s, "tests"), 2));
        }
        return indent(1) + "testSuites:\n" + indent(1) + "[\n" + String.join(",\n", out) + "\n" + indent(1) + "]\n";
    }

    private static String testSuite(String id, List<Json.Obj> testData, List<Json.Obj> tests, int base) {
        StringBuilder b = new StringBuilder(indent(base)).append(convertIdentifier(id)).append(":\n").append(indent(base)).append("{\n");
        if (!testData.isEmpty()) {
            List<String> data = new ArrayList<>();
            for (Json.Obj td : testData) {
                data.add(indent(base + 2) + td.getObj("packageableElementPointer").getString("path") + ":\n"
                        + EmbeddedDataComposer.compose(td.getObj("data"), indent(base + 3)));
            }
            b.append(indent(base + 1)).append("data:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", data))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        if (!tests.isEmpty()) {
            List<String> ts = new ArrayList<>();
            for (Json.Obj t : tests) {
                ts.add(test(t, base + 2));
            }
            b.append(indent(base + 1)).append("tests:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", ts))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        return b.append(indent(base)).append("}").toString();
    }

    private static String test(Json.Obj t, int base) {
        StringBuilder b = new StringBuilder(indent(base)).append(convertIdentifier(t.getString("id"))).append(":\n").append(indent(base)).append("{\n");
        List<Json.Obj> assertions = objs(t, "assertions");
        if (!assertions.isEmpty()) {
            List<String> as = new ArrayList<>();
            for (Json.Obj a : assertions) {
                as.add(TestAssertionComposer.compose(a, indent(base + 2)));
            }
            b.append(indent(base + 1)).append("asserts:\n").append(indent(base + 1)).append("[\n").append(String.join(",\n", as))
                    .append("\n").append(indent(base + 1)).append("]\n");
        }
        return b.append(indent(base)).append("}").toString();
    }
}
