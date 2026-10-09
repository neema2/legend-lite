// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * {@code ###Service}'s service and execution environment as upstream prints them
 * ({@code ServiceGrammarComposerExtension}, {@code HelperServiceGrammarComposer},
 * {@code HelperExecutionEnvironmentGrammarComposer}) -- over the records ({@link Protocol.PService},
 * {@link Protocol.PExecutionEnvironment}; the protocol program's leg 2, step 3).
 */
final class ServiceComposer {

    private ServiceComposer() {
    }

    /** The section's two kinds: services and execution environments. */
    static String element(Protocol.Element e) {
        return switch (e) {
            case Protocol.PService s -> service(s);
            case Protocol.PExecutionEnvironment ee -> executionEnvironment(ee);
            default -> throw Composing.refused("no Service printer for a " + e.getClass().getSimpleName());
        };
    }

    /** {@link #element(Protocol.Element)} of the JSON, read first. */
    static String element(Json.Obj e) {
        return element(Composing.element(e, Protocol.Element.class));
    }

    static String service(Protocol.PService s) {
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("Service", "", s.stereotypes(), s.taggedValues()))
                .append(Composing.elementPath(s.pkg(), s.name())).append("\n{\n");
        if (s.pattern() == null) {
            throw Composing.refused("a service without its pattern");
        }
        b.append(TAB).append("pattern: ").append(convertString(s.pattern(), true)).append(";\n");
        if (s.title() != null) {
            b.append(TAB).append("title: ").append(convertString(s.title(), true)).append(";\n");
        }
        List<String> owners = new ArrayList<>();
        for (String o : s.owners()) {
            owners.add(tab(2) + convertString(o, true));
        }
        if (!owners.isEmpty()) {
            b.append(TAB).append("owners:\n").append(TAB).append("[\n").append(String.join(",\n", owners)).append("\n").append(TAB).append("];\n");
        }
        if (s.ownershipKind() != null) {
            b.append(TAB).append("ownership: ").append(ownership(s)).append(";\n");
        }
        String documentation = s.documentation();
        b.append(TAB).append("documentation: ").append(convertString(documentation != null ? documentation : "", true)).append(";\n");
        b.append(TAB).append("autoActivateUpdates: ").append(Boolean.TRUE.equals(s.autoActivateUpdates()) ? "true" : "false").append(";\n");
        b.append(TAB).append("execution: ").append(execution(s.execution()));
        if (s.testSuites() != null) {
            List<String> suites = new ArrayList<>();
            for (Protocol.PServiceTestSuite suite : s.testSuites()) {
                suites.add(ServiceTestComposer.testSuite(suite));
            }
            b.append(TAB).append("testSuites:\n").append(TAB).append("[\n").append(String.join(",\n", suites)).append("\n").append(TAB).append("]\n");
        }
        Protocol.PLegacyServiceTest test = s.test();
        if (test != null && !ServiceTestComposer.legacyTestEmpty(test)) {
            b.append(TAB).append("test: ").append(ServiceTestComposer.legacyTest(test));
        }
        List<Protocol.PPostValidation> postValidations = s.postValidations() == null ? List.of() : s.postValidations();
        if (!postValidations.isEmpty()) {
            List<String> pvs = new ArrayList<>();
            for (Protocol.PPostValidation pv : postValidations) {
                pvs.add(postValidation(pv));
            }
            b.append(TAB).append("postValidations:\n").append(TAB).append("[\n").append(String.join(",\n", pvs)).append(TAB).append("]\n");
        }
        if (s.mcpServer() != null) {
            b.append(TAB).append("mcpServer: ").append(convertIdentifier(s.mcpServer())).append(";\n");
        }
        return b.append("}").toString();
    }

    /** A deployment owner ({@code DID}) or a user list. */
    private static String ownership(Protocol.PService s) {
        if ("DID".equals(s.ownershipKind())) {
            return "DID { identifier: '" + s.ownershipId() + "' }";
        }
        List<String> users = s.ownershipUsers() == null ? List.of() : s.ownershipUsers();
        return "UserList { users: ['" + String.join("', '", users) + "'] }";
    }

    /** {@code renderServiceExecution}. */
    private static String execution(Protocol.PServiceExecution e) {
        return switch (e) {
            case Protocol.PSingleExecution single -> {
                boolean runtime = single.runtime() != null || single.embeddedRuntime() != null;
                String explicit = single.mapping() != null && runtime
                        ? tab(2) + "mapping: " + single.mapping() + ";\n"
                                + runtime(single.runtime(), single.embeddedRuntime(), 2) + "\n"
                        : "";
                yield "Single\n" + TAB + "{\n" + tab(2) + "query: " + valueSpecification(single.query()) + ";\n" + explicit
                        + TAB + "}\n";
            }
            case Protocol.PMultiExecution multi -> {
                StringBuilder b = new StringBuilder("Multi\n").append(TAB).append("{\n")
                        .append(tab(2)).append("query: ").append(valueSpecification(multi.query())).append(";\n");
                if (multi.executionKey() != null) {
                    b.append(tab(2)).append("key: ").append(convertString(multi.executionKey(), true)).append(";\n");
                }
                List<String> params = new ArrayList<>();
                for (Protocol.PKeyedExecution p : multi.executions() == null ? List.<Protocol.PKeyedExecution>of()
                        : multi.executions()) {
                    params.add(tab(2) + "executions[" + convertString(p.keyValue(), true) + "]:\n" + tab(2) + "{\n"
                            + tab(3) + "mapping: " + mapping(p) + ";\n"
                            + runtime(p.runtime(), p.embeddedRuntime(), 3) + "\n" + tab(2) + "}");
                }
                if (!params.isEmpty()) {
                    b.append(String.join("\n", params)).append("\n");
                }
                yield b.append(TAB).append("}\n").toString();
            }
        };
    }

    private static String mapping(Protocol.PKeyedExecution p) {
        if (p.mapping() == null) {
            throw Composing.refused("an execution '" + p.keyValue() + "' without its mapping");
        }
        return p.mapping();
    }

    /** {@code renderServiceExecutionRuntime}: a runtime pointer, or a runtime written in full. */
    private static String runtime(@com.legend.base.Nullable String pointer,
            Protocol.@com.legend.base.Nullable PEmbeddedRuntime embedded, int base) {
        if (pointer != null) {
            return tab(base) + "runtime: " + pointer + ";";
        }
        if (embedded == null) {
            throw Composing.refused("a service execution without its runtime");
        }
        return tab(base) + "runtime:\n" + tab(base) + "#{" + RuntimeComposer.embedded(embedded, base + 1, "") + "\n"
                + tab(base) + "}#;";
    }

    private static String postValidation(Protocol.PPostValidation pv) {
        List<String> params = new ArrayList<>();
        for (ValueSpecification p : pv.parameters()) {
            params.add(tab(4) + valueSpecification(p));
        }
        List<String> assertions = new ArrayList<>();
        for (Protocol.PPostValidationAssertion a : pv.assertions()) {
            assertions.add(tab(4) + a.id() + ": " + valueSpecification(a.assertion()));
        }
        return tab(2) + "{\n"
                + tab(3) + "description: " + convertString(pv.description(), true) + ";\n"
                + tab(3) + "params:[\n" + String.join(",\n", params) + "\n" + tab(3) + "];\n"
                + tab(3) + "assertions:[\n" + String.join(",\n", assertions) + "\n" + tab(3) + "];\n"
                + tab(2) + "}\n";
    }

    // ---------------------------------------------------------------------
    // ExecutionEnvironment
    // ---------------------------------------------------------------------

    static String executionEnvironment(Protocol.PExecutionEnvironment e) {
        List<String> params = new ArrayList<>();
        for (Protocol.PExecutionParameters p : e.executions()) {
            params.add(switch (p) {
                case Protocol.PMultiKeyedExecution m -> multiParameters(m, 2);
                case Protocol.PKeyedExecution k -> singleParameters(k, 2);
            });
        }
        return "ExecutionEnvironment " + Composing.elementPath(e.pkg(), e.name()) + "\n{\n"
                + TAB + "executions:\n" + TAB + "[\n" + String.join(",\n", params) + "\n" + TAB + "];\n"
                + "}";
    }

    private static String singleParameters(Protocol.PKeyedExecution p, int base) {
        StringBuilder b = new StringBuilder(tab(base)).append(p.keyValue()).append(":\n").append(tab(base)).append("{\n")
                .append(tab(base + 1)).append("mapping: ").append(mapping(p)).append(";\n");
        if (p.runtime() != null || p.embeddedRuntime() != null) {
            b.append(runtime(p.runtime(), p.embeddedRuntime(), base + 1));
        }
        Protocol.PRuntimeComponents components = p.runtimeComponents();
        if (components != null) {
            b.append(tab(base + 1)).append("runtimeComponents:\n").append(tab(base + 1)).append("{\n")
                    .append(tab(base + 2)).append("class: ").append(components.clazz().path()).append(";\n")
                    .append(tab(base + 2)).append("binding: ").append(components.binding().path()).append(";\n")
                    .append(runtime(components.runtime(), null, base + 2))
                    .append("\n").append(tab(base + 1)).append("}");
        }
        return b.append("\n").append(tab(base)).append("}").toString();
    }

    private static String multiParameters(Protocol.PMultiKeyedExecution p, int base) {
        List<String> singles = new ArrayList<>();
        for (Protocol.PKeyedExecution s : p.singles()) {
            singles.add(singleParameters(s, base + 1));
        }
        return tab(base) + p.masterKey() + ":\n" + tab(base) + "[\n" + String.join(",\n", singles) + "\n" + tab(base) + "]";
    }
}
