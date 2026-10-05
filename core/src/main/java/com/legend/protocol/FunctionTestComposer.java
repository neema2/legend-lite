// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * A function's test suites, as upstream prints them after its body
 * ({@code HelperDomainGrammarComposer.renderFunctionTestSuites} and its helpers).
 */
final class FunctionTestComposer {

    private static final String DEFAULT_SUITE = "default";

    private FunctionTestComposer() {
    }

    static String testSuites(Json.Obj function) {
        List<Json.Obj> suites = objs(function, "tests");
        if (suites.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Json.Obj s : suites) {
            out.add(suite(function, s));
        }
        return "\n{\n" + String.join("\n" + (suites.size() > 1 ? "\n" : ""), out) + "\n}";
    }

    private static String suite(Json.Obj function, Json.Obj suite) {
        String id = suite.getString("id");
        boolean named = !DEFAULT_SUITE.equals(id);
        int level = named ? 2 : 1;
        StringBuilder b = new StringBuilder();
        if (named) {
            b.append(tab(1)).append(id).append("\n").append(tab(1)).append("(\n");
        }
        List<String> data = new ArrayList<>();
        for (Json.Obj d : objs(suite, "testData")) {
            data.add(testData(d, level));
        }
        if (!data.isEmpty()) {
            b.append(String.join("\n", data)).append("\n");
        }
        List<String> tests = new ArrayList<>();
        for (Json.Obj t : objs(suite, "tests")) {
            tests.add(test(function, t, level));
        }
        b.append(String.join("\n", tests));
        if (named) {
            b.append("\n").append(tab(1)).append(")");
        }
        return b.toString();
    }

    private static String testData(Json.Obj d, int level) {
        StringBuilder b = new StringBuilder(tab(level)).append(RuntimeComposer.elementPointer(d.getObj("packageableElementPointer"))).append(":");
        Json.Obj data = d.getObj("data");
        String type = Composing.type(data);
        if ("reference".equals(type)) {
            b.append(" ").append(data.getObj("dataElement").getString("path"));
        } else if ("externalFormat".equals(type)) {
            b.append(" ").append(simpleExternalFormat(data));
        } else {
            b.append("\n").append(EmbeddedDataComposer.compose(data, tab(level + 2)));
        }
        return b.append(";").toString();
    }

    private static String test(Json.Obj function, Json.Obj test, int level) {
        String doc = str(test, "doc");
        List<String> params = new ArrayList<>();
        for (Json.Obj p : objs(test, "parameters")) {
            params.add(Composing.valueSpecification(p.get("value")));
        }
        List<Json.Obj> assertions = objs(test, "assertions");
        if (assertions.size() > 1) {
            throw Composing.refused("a function test with more than one assertion (upstream cannot print it)");
        }
        return tab(level) + test.getString("id") + (doc != null ? " " + convertString(doc, true) : "")
                + " | " + FunctionNames.nameWithoutSignature(function) + "(" + String.join(",", params) + ") => "
                + (assertions.isEmpty() ? "" : assertion(assertions.get(0), level)) + ";";
    }

    private static String assertion(Json.Obj a, int level) {
        String type = Composing.type(a);
        if ("equalTo".equals(type)) {
            return Composing.valueSpecification(a.get("expected"));
        }
        if ("equalToJson".equals(type)) {
            return simpleExternalFormat(a.getObj("expected"));
        }
        if ("equalToRelation".equals(type)) {
            return "Relation\n" + EmbeddedDataComposer.alignedRelation(a.getObj("expected"), tab(level), true);
        }
        throw Composing.refused("no composer rule for a function test assertion of _type '" + type + "'");
    }

    /** {@code renderSimpleExternalFormat}: {@code (JSON) '...'}. */
    private static String simpleExternalFormat(Json.Obj data) {
        String contentType = data.getString("contentType");
        String label = "application/json".equals(contentType) ? "JSON" : "application/xml".equals(contentType) ? "XML" : contentType;
        return "(" + label + ") " + convertString(data.getString("data"), true);
    }
}
