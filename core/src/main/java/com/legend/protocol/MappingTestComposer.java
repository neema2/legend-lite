// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * A mapping's tests as upstream prints them: the legacy {@code MappingTests} and the {@code testSuites}
 * ({@code HelperMappingGrammarComposer.renderMappingTest}, {@code renderMappingTestSuite} and the relational
 * extension's test input data).
 */
final class MappingTestComposer {

    /** A SQL input's statements: split at each {@code ;} not escaped by a backslash. */
    private static final Pattern SQL_STATEMENT_END = Pattern.compile("(?<!\\\\);");

    private MappingTestComposer() {
    }

    /** {@code renderMappingTest}. */
    static String legacyTest(Json.Obj test) {
        List<String> data = new ArrayList<>();
        for (Json.Obj d : objs(test, "inputData")) {
            data.add(tab(4) + inputData(d));
        }
        Json.Obj assertion = test.getObj("assert");
        if (!"expectedOutputMappingTestAssert".equals(Composing.type(assertion))) {
            throw Composing.refused("no composer rule for a mapping test assert of _type '" + Composing.type(assertion) + "'");
        }
        return "  " + test.getString("name") + "\n"
                + tab(2) + "(\n"
                + tab(3) + "query: " + Composing.valueSpecification(test.get("query")) + ";\n"
                + tab(3) + "data:\n"
                + tab(3) + "[\n"
                + String.join(",\n", data) + (data.isEmpty() ? "" : "\n")
                + tab(3) + "];\n"
                + tab(3) + "assert: " + convertString(assertion.getString("expectedOutput"), false) + ";\n"
                + tab(2) + ")";
    }

    private static String inputData(Json.Obj d) {
        String type = Composing.type(d);
        if ("object".equals(type)) {
            return "<Object, " + d.getString("inputType") + ", " + Composing.convertPath(d.getString("sourceClass")) + ", "
                    + convertString(d.getString("data"), false) + ">";
        }
        if ("relational".equals(type)) {
            return relationalInputData(d);
        }
        throw Composing.refused("no composer rule for mapping test input data of _type '" + type + "'");
    }

    private static String relationalInputData(Json.Obj d) {
        String inputType = d.getString("inputType");
        String raw = d.getString("data");
        String data;
        if ("SQL".equals(inputType)) {
            List<String> lines = new ArrayList<>();
            for (String l : SQL_STATEMENT_END.split(raw.replace("\r", "").replace("\n", ""))) {
                lines.add(tab(5) + convertString(l + ";\n", true).replace("\\\\;", "\\;"));
            }
            data = "\n" + String.join("+\n", lines);
        } else if ("CSV".equals(inputType)) {
            List<String> lines = new ArrayList<>(List.of(raw.split("\n")));
            lines.add("\n\n");
            List<String> out = new ArrayList<>();
            for (String l : lines) {
                out.add(tab(5) + convertString(l + "\n", true));
            }
            data = "\n" + String.join("+\n", out);
        } else {
            data = raw;
        }
        return "<Relational, " + inputType + ", " + d.getString("database") + ", " + data + "\n" + tab(4) + ">";
    }

    /** {@code renderMappingTestSuite}. */
    static String testSuite(Json.Obj suite) {
        StringBuilder b = new StringBuilder(tab(1)).append(suite.getString("id")).append(":\n").append(tab(2)).append("{\n");
        String doc = str(suite, "doc");
        if (doc != null) {
            b.append(tab(3)).append("doc: ").append(convertString(doc, true)).append(";\n");
        }
        b.append(tab(3)).append("function: ").append(Composing.valueSpecification(suite.get("func"))).append(";\n");
        List<Json.Obj> tests = objs(suite, "tests");
        if (!tests.isEmpty()) {
            List<String> ts = new ArrayList<>();
            for (Json.Obj t : tests) {
                ts.add(test(t));
            }
            b.append(tab(3)).append("tests:\n").append(tab(3)).append("[\n").append(String.join(",\n", ts)).append("\n")
                    .append(tab(3)).append("];\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    /** {@code renderMappingTests}. */
    private static String test(Json.Obj test) {
        StringBuilder b = new StringBuilder(tab(4)).append(test.getString("id")).append(":\n").append(tab(4)).append("{\n");
        String doc = str(test, "doc");
        if (doc != null) {
            b.append(tab(5)).append("doc: ").append(convertString(doc, true)).append(";\n");
        }
        b.append(storeTestData(objs(test, "storeTestData"), 4));
        List<String> asserts = new ArrayList<>();
        for (Json.Obj a : objs(test, "assertions")) {
            asserts.add(TestAssertionComposer.compose(a, tab(6)));
        }
        return b.append(tab(5)).append("asserts:\n").append(tab(5)).append("[\n").append(String.join(",\n", asserts)).append("\n")
                .append(tab(5)).append("];\n").append(tab(4)).append("}").toString();
    }

    /** {@code renderStoreTestData}. */
    static String storeTestData(List<Json.Obj> data, int base) {
        List<String> out = new ArrayList<>();
        for (Json.Obj d : data) {
            out.add(tab(base + 2) + DatabaseComposer.pointerPath(d.get("store")) + ":\n"
                    + EmbeddedDataComposer.compose(d.getObj("data"), tab(base + 3)));
        }
        return tab(base + 1) + "data:\n" + tab(base + 1) + "[\n"
                + (out.isEmpty() ? "" : String.join(",\n", out) + "\n")
                + tab(base + 1) + "];\n";
    }
}
