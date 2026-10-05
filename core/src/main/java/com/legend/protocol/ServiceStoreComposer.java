// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###ServiceStore}'s store, its class mapping, connection and embedded data as upstream prints them
 * ({@code ServiceStoreGrammarComposerExtension}, {@code HelperServiceStoreGrammarComposer},
 * {@code HelperServiceStoreEmbeddedDataComposer} and the content pattern composers).
 */
final class ServiceStoreComposer {

    private static final String SERVICE_MAPPING_PATH_PREFIX = "$service.response";

    /** A type reference's keyword, by its {@code _type}. */
    private static final Map<String, String> SIMPLE_TYPES = Map.of(
            "boolean", "Boolean", "float", "Float", "integer", "Integer", "string", "String");

    private ServiceStoreComposer() {
    }

    static String serviceStore(Json.Obj store) {
        StringBuilder b = new StringBuilder("ServiceStore ").append(elementPath(store)).append("\n(\n");
        String description = str(store, "description");
        if (description != null) {
            b.append("description : '").append(description).append("';\n\n");
        }
        elements(objs(store, "elements"), b, 1);
        return b.append(")").toString();
    }

    private static void elements(List<Json.Obj> elements, StringBuilder b, int base) {
        for (Json.Obj e : elements) {
            if ("service".equals(Composing.type(e))) {
                service(e, b, base);
            }
        }
        for (Json.Obj e : elements) {
            if ("serviceGroup".equals(Composing.type(e))) {
                b.append(tab(base)).append("ServiceGroup ").append(e.getString("id")).append("\n").append(tab(base)).append("(\n")
                        .append(tab(base + 1)).append("path : '").append(e.getString("path")).append("';\n\n");
                elements(objs(e, "elements"), b, base + 1);
                b.append(tab(base)).append(")\n");
            }
        }
    }

    private static void service(Json.Obj s, StringBuilder b, int base) {
        b.append(tab(base)).append("Service ").append(s.getString("id")).append("\n").append(tab(base)).append("(\n")
                .append(tab(base + 1)).append("path : '").append(s.getString("path")).append("';\n");
        Json.Obj body = objOr(s, "requestBody");
        if (body != null) {
            b.append(tab(base + 1)).append("requestBody : ").append(typeReference(body)).append(";\n");
        }
        b.append(tab(base + 1)).append("method : ").append(s.getString("method")).append(";\n");
        List<Json.Obj> params = objs(s, "parameters");
        if (!params.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Obj p : params) {
                ps.add(parameter(p, base + 2));
            }
            b.append(tab(base + 1)).append("parameters :\n").append(tab(base + 1)).append("(\n").append(String.join(",\n", ps)).append("\n")
                    .append(tab(base + 1)).append(");\n");
        }
        b.append(tab(base + 1)).append("response : ").append(typeReference(s.getObj("response"))).append(";\n");
        if (!items(s, "security").isEmpty()) {
            throw Composing.refused("a service store service with security schemes (upstream has no composer for any)");
        }
        b.append(tab(base + 1)).append("security : [];\n").append(tab(base)).append(")\n");
    }

    private static String parameter(Json.Obj p, int base) {
        StringBuilder b = new StringBuilder(tab(base)).append(DatabaseComposer.convertIdentifierDoubleQuoted(p.getString("name")))
                .append(" : ").append(typeReference(p.getObj("type"))).append(" ( location = ")
                .append(p.getString("location").toLowerCase(Locale.ROOT));
        Json.Obj format = objOr(p, "serializationFormat");
        if (format != null) {
            String style = str(format, "style");
            if (style != null) {
                b.append(", style = ").append(style);
            }
            Json.Node explode = Composing.value(format, "explode");
            if (explode != null) {
                b.append(", explode = ").append(RelationalConnectionComposer.raw(explode));
            }
        }
        String enumeration = str(p, "enumeration");
        if (enumeration != null) {
            b.append(", enum = ").append(enumeration);
        }
        Json.Node allowReserved = Composing.value(p, "allowReserved");
        if (allowReserved != null) {
            b.append(", allowReserved = ").append(RelationalConnectionComposer.raw(allowReserved));
        }
        Json.Node required = Composing.value(p, "required");
        if (required != null) {
            b.append(", required = ").append(RelationalConnectionComposer.raw(required));
        }
        return b.append(" )").toString();
    }

    private static String typeReference(Json.Obj t) {
        String type = Composing.type(t);
        String name = SIMPLE_TYPES.get(type);
        if (name == null) {
            if (!"complex".equals(type)) {
                throw Composing.refused("no composer rule for a service store type reference of _type '" + type + "'");
            }
            name = t.getString("type") + " <- " + t.getString("binding");
        }
        boolean list = t.getBoolOr("list", false);
        return (list ? "[" : "") + name + (list ? "]" : "");
    }

    // ---------------------------------------------------------------------
    // Class mapping
    // ---------------------------------------------------------------------

    static String classMapping(Json.Obj cm) {
        StringBuilder b = new StringBuilder(": ServiceStore\n").append(TAB).append("{\n");
        List<Json.Obj> locals = objs(cm, "localMappingProperties");
        for (Json.Obj l : locals) {
            b.append(tab(2)).append("+").append(Composing.convertIdentifier(l.getString("name"))).append(" : ").append(l.getString("type"))
                    .append("[").append(Composing.multiplicity(l.getObj("multiplicity"))).append("];\n");
        }
        if (!locals.isEmpty()) {
            b.append("\n");
        }
        for (Json.Obj sm : objs(cm, "servicesMapping")) {
            serviceMapping(sm, b, 2);
        }
        return b.append(TAB).append("}").toString();
    }

    private static void serviceMapping(Json.Obj sm, StringBuilder b, int base) {
        Json.Obj service = sm.getObj("service");
        b.append(tab(base)).append("~service [").append(service.getString("serviceStore")).append("] ").append(servicePath(service)).append("\n");
        Json.Obj offset = objOr(sm, "pathOffset");
        boolean hasPath = offset != null && !items(offset, "path").isEmpty();
        Json.Obj request = objOr(sm, "requestBuildInfo");
        if (!hasPath && request == null) {
            return;
        }
        b.append(tab(base)).append("(\n");
        if (offset != null && hasPath) {
            List<String> elements = new ArrayList<>();
            for (Json.Obj pe : objs(offset, "path")) {
                List<Json.Node> params = items(pe, "parameters");
                List<String> ps = new ArrayList<>();
                for (Json.Node p : params) {
                    ps.add(Composing.valueSpecification(p));
                }
                elements.add(pe.getString("property") + (params.size() > 1 ? "(" + String.join(", ", ps) + ")" : ""));
            }
            b.append(tab(base + 1)).append("~path ").append(SERVICE_MAPPING_PATH_PREFIX).append(".").append(String.join(".", elements)).append("\n");
        }
        if (request != null) {
            b.append(tab(base + 1)).append("~request\n").append(tab(base + 1)).append("(\n");
            Json.Obj params = objOr(request, "requestParametersBuildInfo");
            if (params != null) {
                List<String> ps = new ArrayList<>();
                for (Json.Obj p : objs(params, "parameterBuildInfoList")) {
                    ps.add(tab(base + 3) + DatabaseComposer.convertIdentifierDoubleQuoted(p.getString("serviceParameter")) + " = "
                            + Composing.lambdaBodyText(p.getObj("transform"), ""));
                }
                b.append(tab(base + 2)).append("parameters\n").append(tab(base + 2)).append("(\n").append(String.join(",\n", ps)).append("\n")
                        .append(tab(base + 2)).append(")\n");
            }
            Json.Obj body = objOr(request, "requestBodyBuildInfo");
            if (body != null) {
                b.append(tab(base + 2)).append("body = ").append(Composing.lambdaBodyText(body.getObj("transform"), "")).append("\n");
            }
            b.append(tab(base + 1)).append(")\n");
        }
        b.append(tab(base)).append(")\n");
    }

    private static String servicePath(Json.Obj service) {
        Json.Obj parent = objOr(service, "parent");
        return (parent == null ? "" : groupPath(parent) + ".") + service.getString("service");
    }

    private static String groupPath(Json.Obj group) {
        Json.Obj parent = objOr(group, "parent");
        return (parent == null ? "" : groupPath(parent) + ".") + group.getString("serviceGroup");
    }

    // ---------------------------------------------------------------------
    // Connection and embedded data
    // ---------------------------------------------------------------------

    static String connection(Json.Obj c, String i) {
        return i + "{\n"
                + i + TAB + "store: " + c.getString("element") + ";\n"
                + i + TAB + "baseUrl: " + convertString(c.getString("baseUrl"), true) + ";\n"
                + i + "}";
    }

    /** {@code visitServiceStoreEmbeddedData}: the stubs, at the content's indentation {@code i}. */
    static String embeddedData(Json.Obj data, String i) {
        List<String> stubs = new ArrayList<>();
        for (Json.Obj stub : objs(data, "serviceStubMappings")) {
            String at = i + TAB;
            stubs.add(at + "{\n" + requestPattern(stub.getObj("requestPattern"), at + TAB) + "\n"
                    + at + TAB + "response:\n" + at + TAB + "{\n" + at + tab(2) + "body:\n"
                    + EmbeddedDataComposer.compose(stub.getObj("responseDefinition").getObj("body"), at + tab(3)) + ";\n"
                    + at + TAB + "};" + "\n" + at + "}");
        }
        return i + "[\n" + String.join(",\n", stubs) + "\n" + i + "]";
    }

    private static String requestPattern(Json.Obj p, String at) {
        StringBuilder b = new StringBuilder(at).append("request:\n").append(at).append("{\n")
                .append(at).append(TAB).append("method: ").append(p.getString("method")).append(";\n");
        String url = str(p, "url");
        if (url != null) {
            b.append(at).append(TAB).append("url: ").append(convertString(url, true)).append(";\n");
        }
        String urlPath = str(p, "urlPath");
        if (urlPath != null) {
            b.append(at).append(TAB).append("urlPath: ").append(convertString(urlPath, true)).append(";\n");
        }
        parameters(b, p, "headerParams", "headerParameters", at);
        parameters(b, p, "queryParams", "queryParameters", at);
        List<Json.Obj> bodyPatterns = objs(p, "bodyPatterns");
        if (!bodyPatterns.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Obj bp : bodyPatterns) {
                ps.add(contentPattern(bp, at + tab(2)));
            }
            b.append(at).append(TAB).append("bodyPatterns:\n").append(at).append(TAB).append("[\n").append(String.join(",\n", ps)).append("\n")
                    .append(at).append(TAB).append("];\n");
        }
        return b.append(at).append("};").toString();
    }

    private static void parameters(StringBuilder b, Json.Obj p, String key, String keyword, String at) {
        Json.Obj params = objOr(p, key);
        if (params == null || params.fields().isEmpty()) {
            return;
        }
        List<String> ps = new ArrayList<>();
        for (Map.Entry<String, Json.Node> e : params.fields().entrySet()) {
            ps.add(at + tab(2) + e.getKey() + ":\n" + contentPattern(Composing.obj(e.getValue(), "pattern"), at + tab(3)));
        }
        b.append(at).append(TAB).append(keyword).append(":\n").append(at).append(TAB).append("{\n").append(String.join(",\n", ps)).append("\n")
                .append(at).append(TAB).append("};\n");
    }

    /** {@code HelperContentPatternGrammarComposer.composeContentPattern}. */
    private static String contentPattern(Json.Obj pattern, String i) {
        String type = Composing.type(pattern);
        String inner = i + TAB;
        String keyword;
        String content;
        if ("equalTo".equals(type)) {
            keyword = "EqualTo";
            content = inner + "expected: " + convertString(pattern.getString("expectedValue"), true) + ";";
        } else if ("equalToJson".equals(type)) {
            keyword = "EqualToJson";
            content = inner + "expected:" + convertString(pattern.getString("expectedValue"), false) + ";";
        } else {
            throw Composing.refused("no composer rule for a content pattern of _type '" + type + "'");
        }
        return i + keyword + "\n" + i + "#{\n" + content + "\n" + i + "}#";
    }
}
