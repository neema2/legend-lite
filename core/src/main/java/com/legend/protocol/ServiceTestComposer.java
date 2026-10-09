// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * A service's tests as upstream prints them ({@code HelperServiceGrammarComposer}): its test suites, in
 * the block form or the flat form, and the legacy {@code test:}.
 */
final class ServiceTestComposer {

    private ServiceTestComposer() {
    }

    /** {@code renderServiceTestSuite}: the flat form when the suite carries service test data. */
    static String testSuite(Json.Obj suite) {
        Json.Obj testData = objOr(suite, "testData");
        if (testData != null && !objs(testData, "serviceTestData").isEmpty()) {
            return flatSuite(suite, testData);
        }
        return blockSuite(suite, testData);
    }

    private static String blockSuite(Json.Obj suite, @com.legend.base.Nullable Json.Obj testData) {
        StringBuilder b = new StringBuilder(tab(2)).append(convertIdentifier(suite.getString("id"))).append(":\n").append(tab(2)).append("{\n");
        if (testData != null) {
            b.append(tab(3)).append("data:\n").append(tab(3)).append("[\n");
            List<Json.Obj> connections = objs(testData, "connectionsTestData");
            if (!connections.isEmpty()) {
                List<String> cs = new ArrayList<>();
                for (Json.Obj c : connections) {
                    cs.add(tab(5) + c.getString("id") + ":\n" + EmbeddedDataComposer.compose(c.getObj("data"), tab(6)));
                }
                b.append(tab(4)).append("connections:\n").append(tab(4)).append("[\n").append(String.join(",\n", cs)).append("\n")
                        .append(tab(4)).append("]\n");
            }
            b.append(tab(3)).append("]\n");
        }
        if (Composing.value(suite, "tests") != null) {
            List<String> ts = new ArrayList<>();
            for (Json.Obj t : objs(suite, "tests")) {
                ts.add(blockTest(t, 4));
            }
            b.append(tab(3)).append("tests:\n").append(tab(3)).append("[\n").append(String.join(",\n", ts)).append("\n").append(tab(3)).append("]\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    private static String blockTest(Json.Obj test, int base) {
        StringBuilder b = new StringBuilder(tab(base)).append(convertIdentifier(test.getString("id"))).append(":\n").append(tab(base)).append("{\n");
        String format = str(test, "serializationFormat");
        if (format != null) {
            b.append(tab(base + 1)).append("serializationFormat: ").append(format).append(";\n");
        }
        List<Json.Obj> params = objs(test, "parameters");
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Obj p : params) {
                ps.add(tab(base + 2) + p.getString("name") + " = " + valueSpecification(p.get("value")));
            }
            b.append(tab(base + 1)).append("parameters:\n").append(tab(base + 1)).append("[\n").append(String.join(",\n", ps)).append("\n")
                    .append(tab(base + 1)).append("]\n");
        }
        List<String> keys = keys(test);
        if (!keys.isEmpty()) {
            b.append(tab(base + 1)).append("keys:\n").append(tab(base + 1)).append("[\n").append(tab(base + 2)).append(String.join(",\n", keys))
                    .append("\n").append(tab(base + 1)).append("];\n");
        }
        if (Composing.value(test, "assertions") != null) {
            List<String> as = new ArrayList<>();
            for (Json.Obj a : objs(test, "assertions")) {
                as.add(TestAssertionComposer.compose(a, tab(base + 2)));
            }
            b.append(tab(base + 1)).append("asserts:\n").append(tab(base + 1)).append("[\n").append(String.join(",\n", as)).append("\n")
                    .append(tab(base + 1)).append("]\n");
        }
        return b.append(tab(base)).append("}").toString();
    }

    private static List<String> keys(Json.Obj test) {
        List<String> out = new ArrayList<>();
        for (String k : test.getStringArrayOr("keys", List.of())) {
            out.add(convertString(k, true));
        }
        return out;
    }

    private static String flatSuite(Json.Obj suite, Json.Obj testData) {
        String doc = str(suite, "doc");
        StringBuilder b = new StringBuilder(tab(2)).append(convertIdentifier(suite.getString("id")))
                .append(doc != null ? " " + convertString(doc, true) : "").append("\n").append(tab(2)).append("(\n");
        for (Json.Obj r : objs(testData, "serviceTestData")) {
            b.append(resolver(r, 3)).append("\n");
        }
        for (Json.Obj t : objs(suite, "tests")) {
            b.append(atomicTest(t, 3)).append("\n");
        }
        return b.append(tab(2)).append(")").toString();
    }

    private static String resolver(Json.Obj r, int base) {
        String path = r.getObj("elementPointer").getString("path");
        String type = Composing.type(r);
        if ("referenceDataResolver".equals(type)) {
            return tab(base) + path + ";";
        }
        if ("baseDataResolver".equals(type)) {
            return tab(base) + path + ":\n" + EmbeddedDataComposer.compose(r.getObj("data"), tab(base + 1)) + ";";
        }
        throw Composing.refused("no composer rule for a data resolver of _type '" + type + "'");
    }

    private static String atomicTest(Json.Obj test, int base) {
        StringBuilder b = new StringBuilder(tab(base)).append(convertIdentifier(test.getString("id")));
        String doc = str(test, "doc");
        if (doc != null) {
            b.append(" ").append(convertString(doc, true));
        }
        List<Json.Obj> params = objs(test, "parameters");
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Obj p : params) {
                ps.add(p.getString("name") + " = " + valueSpecification(p.get("value")));
            }
            b.append(" (").append(String.join(", ", ps)).append(")");
        }
        List<String> keys = keys(test);
        if (!keys.isEmpty()) {
            b.append(" [").append(String.join(", ", keys)).append("]");
        }
        String format = str(test, "serializationFormat");
        if (format != null) {
            b.append(" : ").append(format);
        }
        b.append(" =>\n");
        List<Json.Obj> assertions = objs(test, "assertions");
        if (assertions.size() != 1) {
            throw Composing.refused("a flat service test with " + assertions.size() + " assertions (upstream cannot print it)");
        }
        Json.Obj a = assertions.get(0);
        String type = Composing.type(a);
        if ("equalToRelation".equals(type)) {
            b.append(tab(base + 1)).append("Relation\n").append(EmbeddedDataComposer.alignedRelation(a.getObj("expected"), tab(base + 1), true));
        } else if ("equalToJson".equals(type)) {
            b.append(EmbeddedDataComposer.compose(a.getObj("expected"), tab(base + 1)));
        } else {
            throw Composing.refused("a flat service test asserting by _type '" + type + "' (upstream cannot print it)");
        }
        return b.append(";").toString();
    }

    // ---------------------------------------------------------------------
    // The legacy test
    // ---------------------------------------------------------------------

    /** {@code isServiceTestEmpty}. */
    static boolean legacyTestEmpty(Json.Obj test) {
        String type = Composing.type(test);
        if ("singleExecutionTest".equals(type)) {
            return items(test, "asserts").isEmpty();
        }
        if ("multiExecutionTest".equals(type)) {
            for (Json.Obj t : objs(test, "tests")) {
                if (!items(t, "asserts").isEmpty()) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static String legacyTest(Json.Obj test) {
        String type = Composing.type(test);
        if ("singleExecutionTest".equals(type)) {
            return "Single\n" + TAB + "{\n"
                    + tab(2) + "data: " + convertString(test.getString("data"), true) + ";\n"
                    + tab(2) + "asserts:\n" + testContainers(objs(test, "asserts"), 2) + "\n"
                    + TAB + "}\n";
        }
        if ("multiExecutionTest".equals(type)) {
            List<String> tests = new ArrayList<>();
            for (Json.Obj t : objs(test, "tests")) {
                tests.add(tab(2) + "tests[" + convertString(t.getString("key"), true) + "]:\n" + tab(2) + "{\n"
                        + tab(3) + "data: " + convertString(t.getString("data"), true) + ";\n"
                        + tab(3) + "asserts:\n" + testContainers(objs(t, "asserts"), 3)
                        + "\n" + tab(2) + "}");
            }
            return "Multi\n" + TAB + "{\n" + String.join("\n", tests) + "\n" + TAB + "}\n";
        }
        throw Composing.refused("no composer rule for a legacy service test of _type '" + type + "'");
    }

    private static String testContainers(List<Json.Obj> containers, int indent) {
        List<String> out = new ArrayList<>();
        for (Json.Obj c : containers) {
            List<String> params = new ArrayList<>();
            for (Json.Node p : items(c, "parametersValues")) {
                params.add(PureComposer.legacyServiceParameter(p));
            }
            out.add(tab(indent + 1) + "{ [" + String.join(", ", params) + "], " + valueSpecification(c.get("assert")) + " }");
        }
        return tab(indent) + "[\n" + String.join(",\n", out) + (out.isEmpty() ? "" : "\n") + tab(indent) + "];";
    }
}
