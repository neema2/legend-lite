// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * {@code ###Service}'s service and execution environment as upstream prints them
 * ({@code ServiceGrammarComposerExtension}, {@code HelperServiceGrammarComposer},
 * {@code HelperExecutionEnvironmentGrammarComposer}).
 */
final class ServiceComposer {

    private ServiceComposer() {
    }

    /** The section's two kinds: services and execution environments. */
    static String element(Json.Obj e) {
        return "executionEnvironmentInstance".equals(Composing.type(e)) ? executionEnvironment(e) : service(e);
    }

    static String service(Json.Obj s) {
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("Service", "", s)).append(elementPath(s)).append("\n{\n");
        b.append(TAB).append("pattern: ").append(convertString(s.getString("pattern"), true)).append(";\n");
        String title = str(s, "title");
        if (title != null) {
            b.append(TAB).append("title: ").append(convertString(title, true)).append(";\n");
        }
        List<String> owners = new ArrayList<>();
        for (String o : s.getStringArrayOr("owners", List.of())) {
            owners.add(tab(2) + convertString(o, true));
        }
        if (!owners.isEmpty()) {
            b.append(TAB).append("owners:\n").append(TAB).append("[\n").append(String.join(",\n", owners)).append("\n").append(TAB).append("];\n");
        }
        Json.Obj ownership = objOr(s, "ownership");
        if (ownership != null) {
            b.append(TAB).append("ownership: ").append(ownership(ownership)).append(";\n");
        }
        String documentation = str(s, "documentation");
        b.append(TAB).append("documentation: ").append(convertString(documentation != null ? documentation : "", true)).append(";\n");
        b.append(TAB).append("autoActivateUpdates: ").append(s.getBoolOr("autoActivateUpdates", false) ? "true" : "false").append(";\n");
        b.append(TAB).append("execution: ").append(execution(s.getObj("execution")));
        if (Composing.value(s, "testSuites") != null) {
            List<String> suites = new ArrayList<>();
            for (Json.Obj suite : objs(s, "testSuites")) {
                suites.add(ServiceTestComposer.testSuite(suite));
            }
            b.append(TAB).append("testSuites:\n").append(TAB).append("[\n").append(String.join(",\n", suites)).append("\n").append(TAB).append("]\n");
        }
        Json.Obj test = objOr(s, "test");
        if (test != null && !ServiceTestComposer.legacyTestEmpty(test)) {
            b.append(TAB).append("test: ").append(ServiceTestComposer.legacyTest(test));
        }
        List<Json.Obj> postValidations = objs(s, "postValidations");
        if (!postValidations.isEmpty()) {
            List<String> pvs = new ArrayList<>();
            for (Json.Obj pv : postValidations) {
                pvs.add(postValidation(pv));
            }
            b.append(TAB).append("postValidations:\n").append(TAB).append("[\n").append(String.join(",\n", pvs)).append(TAB).append("]\n");
        }
        String mcpServer = str(s, "mcpServer");
        if (mcpServer != null) {
            b.append(TAB).append("mcpServer: ").append(convertIdentifier(mcpServer)).append(";\n");
        }
        return b.append("}").toString();
    }

    private static String ownership(Json.Obj o) {
        String type = Composing.type(o);
        if ("deploymentOwnership".equals(type)) {
            return "DID { identifier: '" + o.getString("identifier") + "' }";
        }
        if ("userListOwnership".equals(type)) {
            return "UserList { users: ['" + String.join("', '", o.getStringArrayOr("users", List.of())) + "'] }";
        }
        throw Composing.refused("no composer rule for a service ownership of _type '" + type + "' (upstream prints its can't-transform comment)");
    }

    /** {@code renderServiceExecution}. */
    private static String execution(Json.Obj e) {
        String type = Composing.type(e);
        if ("pureSingleExecution".equals(type)) {
            String mapping = str(e, "mapping");
            Json.Obj runtime = objOr(e, "runtime");
            String explicit = mapping != null && runtime != null
                    ? tab(2) + "mapping: " + mapping + ";\n" + runtime(runtime, 2) + "\n" : "";
            return "Single\n" + TAB + "{\n" + tab(2) + "query: " + valueSpecification(e.get("func")) + ";\n" + explicit + TAB + "}\n";
        }
        if ("pureMultiExecution".equals(type)) {
            StringBuilder b = new StringBuilder("Multi\n").append(TAB).append("{\n")
                    .append(tab(2)).append("query: ").append(valueSpecification(e.get("func"))).append(";\n");
            String key = str(e, "executionKey");
            if (key != null) {
                b.append(tab(2)).append("key: ").append(convertString(key, true)).append(";\n");
            }
            List<String> params = new ArrayList<>();
            for (Json.Obj p : objs(e, "executionParameters")) {
                params.add(tab(2) + "executions[" + convertString(p.getString("key"), true) + "]:\n" + tab(2) + "{\n"
                        + tab(3) + "mapping: " + p.getString("mapping") + ";\n" + runtime(p.getObj("runtime"), 3) + "\n" + tab(2) + "}");
            }
            if (!params.isEmpty()) {
                b.append(String.join("\n", params)).append("\n");
            }
            return b.append(TAB).append("}\n").toString();
        }
        throw Composing.refused("no composer rule for a service execution of _type '" + type + "'");
    }

    /** {@code renderServiceExecutionRuntime}. */
    static String runtime(Json.Obj runtime, int base) {
        String type = Composing.type(runtime);
        if ("runtimePointer".equals(type)) {
            return tab(base) + "runtime: " + runtime.getString("runtime") + ";";
        }
        if ("engineRuntime".equals(type)) {
            return tab(base) + "runtime:\n" + tab(base) + "#{"
                    + RuntimeComposer.embedded(ServiceReader.embeddedRuntime(runtime), base + 1, "") + "\n" + tab(base)
                    + "}#;";
        }
        throw Composing.refused("no composer rule for a service runtime of _type '" + type + "'");
    }

    private static String postValidation(Json.Obj pv) {
        List<String> params = new ArrayList<>();
        for (Json.Node p : Composing.items(pv, "parameters")) {
            params.add(tab(4) + valueSpecification(p));
        }
        List<String> assertions = new ArrayList<>();
        for (Json.Obj a : objs(pv, "assertions")) {
            assertions.add(tab(4) + a.getString("id") + ": " + valueSpecification(a.get("assertion")));
        }
        return tab(2) + "{\n"
                + tab(3) + "description: " + convertString(pv.getString("description"), true) + ";\n"
                + tab(3) + "params:[\n" + String.join(",\n", params) + "\n" + tab(3) + "];\n"
                + tab(3) + "assertions:[\n" + String.join(",\n", assertions) + "\n" + tab(3) + "];\n"
                + tab(2) + "}\n";
    }

    // ---------------------------------------------------------------------
    // ExecutionEnvironment
    // ---------------------------------------------------------------------

    static String executionEnvironment(Json.Obj e) {
        List<String> params = new ArrayList<>();
        for (Json.Obj p : objs(e, "executionParameters")) {
            params.add("multiExecutionParameters".equals(Composing.type(p)) ? multiParameters(p, 2) : singleParameters(p, 2));
        }
        return "ExecutionEnvironment " + elementPath(e) + "\n{\n"
                + TAB + "executions:\n" + TAB + "[\n" + String.join(",\n", params) + "\n" + TAB + "];\n"
                + "}";
    }

    private static String singleParameters(Json.Obj p, int base) {
        StringBuilder b = new StringBuilder(tab(base)).append(p.getString("key")).append(":\n").append(tab(base)).append("{\n")
                .append(tab(base + 1)).append("mapping: ").append(p.getString("mapping")).append(";\n");
        Json.Obj runtime = objOr(p, "runtime");
        if (runtime != null) {
            b.append(runtime(runtime, base + 1));
        }
        Json.Obj components = objOr(p, "runtimeComponents");
        if (components != null) {
            b.append(tab(base + 1)).append("runtimeComponents:\n").append(tab(base + 1)).append("{\n")
                    .append(tab(base + 2)).append("class: ").append(components.getObj("clazz").getString("path")).append(";\n")
                    .append(tab(base + 2)).append("binding: ").append(components.getObj("binding").getString("path")).append(";\n")
                    .append(runtime(components.getObj("runtime"), base + 2))
                    .append("\n").append(tab(base + 1)).append("}");
        }
        return b.append("\n").append(tab(base)).append("}").toString();
    }

    private static String multiParameters(Json.Obj p, int base) {
        List<String> singles = new ArrayList<>();
        for (Json.Obj s : objs(p, "singleExecutionParameters")) {
            singles.add(singleParameters(s, base + 1));
        }
        return tab(base) + p.getString("masterKey") + ":\n" + tab(base) + "[\n" + String.join(",\n", singles) + "\n" + tab(base) + "]";
    }
}
