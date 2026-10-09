// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###ServiceStore}'s store, its class mapping, connection and embedded data as upstream prints them
 * ({@code ServiceStoreGrammarComposerExtension}, {@code HelperServiceStoreGrammarComposer},
 * {@code HelperServiceStoreEmbeddedDataComposer} and the content pattern composers) -- over the records
 * ({@link Protocol.PServiceStoreDefinition}, {@link Protocol.PServiceStoreClassMapping},
 * {@link Protocol.PServiceStoreConnection}, {@link Protocol.PServiceStoreData}; the protocol program's leg 2, step 3).
 */
final class ServiceStoreComposer {

    private static final String SERVICE_MAPPING_PATH_PREFIX = "$service.response";

    /** The primitive type references upstream prints (the reader gives them the grammar's initial capital). */
    private static final Set<String> SIMPLE_TYPES = Set.of("Boolean", "Float", "Integer", "String");

    private ServiceStoreComposer() {
    }

    static String serviceStore(Protocol.PServiceStoreDefinition store) {
        StringBuilder b = new StringBuilder("ServiceStore ").append(Composing.elementPath(store.pkg(), store.name()))
                .append("\n(\n");
        if (store.description() != null) {
            b.append("description : '").append(store.description()).append("';\n\n");
        }
        elements(store.elements(), b, 1);
        return b.append(")").toString();
    }

    /** The services first, then the groups, each group's own elements nested one level deeper. */
    private static void elements(List<Protocol.PServiceStoreElement> elements, StringBuilder b, int base) {
        for (Protocol.PServiceStoreElement e : elements) {
            if (e instanceof Protocol.PSsService s) {
                service(s, b, base);
            }
        }
        for (Protocol.PServiceStoreElement e : elements) {
            if (e instanceof Protocol.PSsServiceGroup g) {
                b.append(tab(base)).append("ServiceGroup ").append(g.id()).append("\n").append(tab(base)).append("(\n")
                        .append(tab(base + 1)).append("path : '").append(g.path()).append("';\n\n");
                elements(g.elements(), b, base + 1);
                b.append(tab(base)).append(")\n");
            }
        }
    }

    private static void service(Protocol.PSsService s, StringBuilder b, int base) {
        b.append(tab(base)).append("Service ").append(s.id()).append("\n").append(tab(base)).append("(\n")
                .append(tab(base + 1)).append("path : '").append(s.path()).append("';\n");
        if (s.requestBody() != null) {
            b.append(tab(base + 1)).append("requestBody : ").append(typeReference(s.requestBody())).append(";\n");
        }
        b.append(tab(base + 1)).append("method : ").append(s.method()).append(";\n");
        List<Protocol.PSsParam> params = s.parameters() == null ? List.of() : s.parameters();
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Protocol.PSsParam p : params) {
                ps.add(parameter(p, base + 2));
            }
            b.append(tab(base + 1)).append("parameters :\n").append(tab(base + 1)).append("(\n").append(String.join(",\n", ps)).append("\n")
                    .append(tab(base + 1)).append(");\n");
        }
        b.append(tab(base + 1)).append("response : ").append(typeReference(s.response())).append(";\n");
        if (!s.security().isEmpty()) {
            throw Composing.refused("a service store service with security schemes (upstream has no composer for any)");
        }
        b.append(tab(base + 1)).append("security : [];\n").append(tab(base)).append(")\n");
    }

    private static String parameter(Protocol.PSsParam p, int base) {
        StringBuilder b = new StringBuilder(tab(base)).append(DatabaseComposer.convertIdentifierDoubleQuoted(p.name()))
                .append(" : ").append(typeReference(p.type())).append(" ( location = ")
                .append(p.location().toLowerCase(Locale.ROOT));
        if (p.style() != null) {
            b.append(", style = ").append(p.style());
        }
        if (p.explode() != null) {
            b.append(", explode = ").append(p.explode());
        }
        if (p.enumeration() != null) {
            b.append(", enum = ").append(p.enumeration());
        }
        if (p.allowReserved() != null) {
            b.append(", allowReserved = ").append(p.allowReserved());
        }
        if (p.required() != null) {
            b.append(", required = ").append(p.required());
        }
        return b.append(" )").toString();
    }

    private static String typeReference(Protocol.PSsTypeRef t) {
        String name;
        if (t.primitive() != null) {
            if (!SIMPLE_TYPES.contains(t.primitive())) {
                throw Composing.refused("no composer rule for a service store type reference of _type '"
                        + t.primitive().toLowerCase(Locale.ROOT) + "'");
            }
            name = t.primitive();
        } else {
            name = t.complexType() + " <- " + t.binding();
        }
        return (t.list() ? "[" : "") + name + (t.list() ? "]" : "");
    }

    // ---------------------------------------------------------------------
    // Class mapping
    // ---------------------------------------------------------------------

    static String classMapping(Protocol.PServiceStoreClassMapping cm) {
        StringBuilder b = new StringBuilder(": ServiceStore\n").append(TAB).append("{\n");
        for (Protocol.PServiceStoreLocalProp l : cm.localProps()) {
            b.append(tab(2)).append("+").append(Composing.convertIdentifier(l.name())).append(" : ").append(l.type())
                    .append("[").append(Composing.multiplicity(l.lowerBound(),
                            l.upperBound() == null ? null : l.upperBound().longValue())).append("];\n");
        }
        if (!cm.localProps().isEmpty()) {
            b.append("\n");
        }
        for (Protocol.PServiceMapping sm : cm.services()) {
            serviceMapping(sm, b, 2);
        }
        return b.append(TAB).append("}").toString();
    }

    private static void serviceMapping(Protocol.PServiceMapping sm, StringBuilder b, int base) {
        Protocol.PServicePtr service = sm.service();
        List<String> segments = new ArrayList<>();
        for (Protocol.PServiceSegment s : service.segments()) {
            segments.add(s.name());
        }
        b.append(tab(base)).append("~service [").append(service.serviceStore()).append("] ")
                .append(String.join(".", segments)).append("\n");
        List<String> path = sm.pathOffset() == null ? List.of() : sm.pathOffset().propertyPath();
        Protocol.PRequestBuildInfo request = sm.request();
        if (path.isEmpty() && request == null) {
            return;
        }
        b.append(tab(base)).append("(\n");
        if (!path.isEmpty()) {
            b.append(tab(base + 1)).append("~path ").append(SERVICE_MAPPING_PATH_PREFIX).append(".")
                    .append(String.join(".", path)).append("\n");
        }
        if (request != null) {
            b.append(tab(base + 1)).append("~request\n").append(tab(base + 1)).append("(\n");
            Protocol.PParametersBuildInfo params = request.parameters();
            if (params != null) {
                List<String> ps = new ArrayList<>();
                for (Protocol.PParameterBuildInfo p : params.entries()) {
                    ps.add(tab(base + 3) + DatabaseComposer.convertIdentifierDoubleQuoted(p.serviceParameter()) + " = "
                            + Composing.lambdaBodyText(List.of(p.transform()), ""));
                }
                b.append(tab(base + 2)).append("parameters\n").append(tab(base + 2)).append("(\n").append(String.join(",\n", ps)).append("\n")
                        .append(tab(base + 2)).append(")\n");
            }
            Protocol.PBodyBuildInfo body = request.body();
            if (body != null) {
                b.append(tab(base + 2)).append("body = ").append(Composing.lambdaBodyText(List.of(body.transform()), ""))
                        .append("\n");
            }
            b.append(tab(base + 1)).append(")\n");
        }
        b.append(tab(base)).append(")\n");
    }

    // ---------------------------------------------------------------------
    // Connection and embedded data
    // ---------------------------------------------------------------------

    static String connection(Protocol.PServiceStoreConnection c, String i) {
        if (c.element() == null) {
            throw Composing.refused("a ServiceStoreConnection with no store");
        }
        return i + "{\n"
                + i + TAB + "store: " + c.element() + ";\n"
                + i + TAB + "baseUrl: " + convertString(c.baseUrl(), true) + ";\n"
                + i + "}";
    }

    /** {@code visitServiceStoreEmbeddedData}: the stubs, at the content's indentation {@code i}. */
    static String embeddedData(Protocol.PServiceStoreData data, String i) {
        List<String> stubs = new ArrayList<>();
        for (Protocol.PServiceStub stub : data.stubs()) {
            String at = i + TAB;
            stubs.add(at + "{\n" + requestPattern(stub, at + TAB) + "\n"
                    + at + TAB + "response:\n" + at + TAB + "{\n" + at + tab(2) + "body:\n"
                    + EmbeddedDataComposer.compose(stub.body(), at + tab(3)) + ";\n"
                    + at + TAB + "};" + "\n" + at + "}");
        }
        return i + "[\n" + String.join(",\n", stubs) + "\n" + i + "]";
    }

    private static String requestPattern(Protocol.PServiceStub p, String at) {
        StringBuilder b = new StringBuilder(at).append("request:\n").append(at).append("{\n")
                .append(at).append(TAB).append("method: ").append(p.method()).append(";\n");
        if (p.url() != null) {
            b.append(at).append(TAB).append("url: ").append(convertString(p.url(), true)).append(";\n");
        }
        if (p.urlPath() != null) {
            b.append(at).append(TAB).append("urlPath: ").append(convertString(p.urlPath(), true)).append(";\n");
        }
        parameters(b, p.headerParams(), "headerParameters", at);
        parameters(b, p.queryParams(), "queryParameters", at);
        List<Protocol.PStringValuePattern> bodyPatterns = p.bodyPatterns() == null ? List.of() : p.bodyPatterns();
        if (!bodyPatterns.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Protocol.PStringValuePattern bp : bodyPatterns) {
                ps.add(contentPattern(bp, at + tab(2)));
            }
            b.append(at).append(TAB).append("bodyPatterns:\n").append(at).append(TAB).append("[\n").append(String.join(",\n", ps)).append("\n")
                    .append(at).append(TAB).append("];\n");
        }
        return b.append(at).append("};").toString();
    }

    private static void parameters(StringBuilder b, @com.legend.base.Nullable List<Protocol.PStubParam> params,
            String keyword, String at) {
        if (params == null || params.isEmpty()) {
            return;
        }
        List<String> ps = new ArrayList<>();
        for (Protocol.PStubParam p : params) {
            ps.add(at + tab(2) + p.name() + ":\n" + contentPattern(p.pattern(), at + tab(3)));
        }
        b.append(at).append(TAB).append(keyword).append(":\n").append(at).append(TAB).append("{\n").append(String.join(",\n", ps)).append("\n")
                .append(at).append(TAB).append("};\n");
    }

    /** {@code HelperContentPatternGrammarComposer.composeContentPattern}. */
    private static String contentPattern(Protocol.PStringValuePattern pattern, String i) {
        String inner = i + TAB;
        String keyword;
        String content;
        if ("equalTo".equals(pattern.type())) {
            keyword = "EqualTo";
            content = inner + "expected: " + convertString(pattern.expectedValue(), true) + ";";
        } else if ("equalToJson".equals(pattern.type())) {
            keyword = "EqualToJson";
            content = inner + "expected:" + convertString(pattern.expectedValue(), false) + ";";
        } else {
            throw Composing.refused("no composer rule for a content pattern of _type '" + pattern.type() + "'");
        }
        return i + keyword + "\n" + i + "#{\n" + content + "\n" + i + "}#";
    }
}
