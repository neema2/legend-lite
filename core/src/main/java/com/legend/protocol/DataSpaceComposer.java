// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###DataSpace}'s data space as upstream prints it ({@code DataSpaceGrammarComposerExtension.renderDataSpace}).
 */
final class DataSpaceComposer {

    private DataSpaceComposer() {
    }

    static String dataSpace(Json.Obj d) {
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("DataSpace", "", d)).append(elementPath(d)).append("\n{\n");
        List<Json.Obj> contexts = objs(d, "executionContexts");
        if (!contexts.isEmpty()) {
            List<String> cs = new ArrayList<>();
            for (Json.Obj c : contexts) {
                cs.add(executionContext(c));
            }
            b.append(TAB).append("executionContexts:\n").append(TAB).append("[\n").append(String.join(",\n", cs)).append("\n").append(TAB).append("];\n");
        }
        optionalString(b, TAB, "defaultExecutionContext", str(d, "defaultExecutionContext"));
        optionalString(b, TAB, "title", str(d, "title"));
        optionalString(b, TAB, "description", str(d, "description"));
        List<String> diagrams = null;
        if (Composing.value(d, "diagrams") != null || Composing.value(d, "featuredDiagrams") != null) {
            diagrams = new ArrayList<>();
            for (Json.Obj g : objs(d, "diagrams")) {
                diagrams.add(diagram(str(g, "title"), str(g, "description"), g.getObj("diagram").getString("path")));
            }
            for (Json.Obj g : objs(d, "featuredDiagrams")) {
                diagrams.add(diagram("", null, g.getString("path")));
            }
        }
        if (diagrams != null) {
            list(b, "diagrams", diagrams);
        }
        if (Composing.value(d, "elements") != null) {
            List<Json.Obj> elements = objs(d, "elements");
            List<String> es = new ArrayList<>();
            for (Json.Obj e : elements) {
                es.add((e.getBoolOr("exclude", false) ? "-" : "") + e.getString("path"));
            }
            b.append(TAB).append("elements:").append(es.isEmpty() ? " []"
                    : "\n" + TAB + "[\n" + tab(2) + String.join(",\n" + tab(2), es) + "\n" + TAB + "]").append(";\n");
        }
        if (Composing.value(d, "executables") != null) {
            List<String> xs = new ArrayList<>();
            for (Json.Obj x : objs(d, "executables")) {
                xs.add(executable(x));
            }
            list(b, "executables", xs);
        }
        Json.Obj support = objOr(d, "supportInfo");
        if (support != null) {
            b.append(TAB).append("supportInfo: ").append(supportInfo(support)).append(";\n");
        }
        Json.Obj metadata = objOr(d, "operationalMetadata");
        if (metadata != null) {
            b.append(TAB).append("operationalMetadata: ").append(operationalMetadata(metadata)).append(";\n");
        }
        return b.append("}").toString();
    }

    private static void optionalString(StringBuilder b, String indent, String key, @com.legend.base.Nullable String value) {
        if (value != null) {
            b.append(indent).append(key).append(": ").append(convertString(value, true)).append(";\n");
        }
    }

    private static void list(StringBuilder b, String key, List<String> items) {
        b.append(TAB).append(key).append(":").append(items.isEmpty() ? " []"
                : "\n" + TAB + "[\n" + String.join(",\n", items) + "\n" + TAB + "]").append(";\n");
    }

    private static String executionContext(Json.Obj c) {
        StringBuilder b = new StringBuilder(tab(2)).append("{\n");
        b.append(tab(3)).append("name: ").append(convertString(c.getString("name"), true)).append(";\n");
        optionalString(b, tab(3), "title", str(c, "title"));
        optionalString(b, tab(3), "description", str(c, "description"));
        Json.Obj mapping = objOr(c, "mapping");
        if (mapping != null) {
            b.append(tab(3)).append("mapping: ").append(convertPath(mapping.getString("path"))).append(";\n");
        }
        Json.Obj provider = objOr(c, "mappingProvider");
        if (provider != null) {
            List<String> keys = provider.getStringArrayOr("keys", List.of());
            b.append(tab(3)).append("mappingProvider: ").append(convertPath(provider.getObj("element").getString("path")))
                    .append(keys.isEmpty() ? "" : "." + String.join(",", keys)).append(";\n");
        }
        Json.Obj runtime = objOr(c, "defaultRuntime");
        if (runtime != null) {
            b.append(tab(3)).append("defaultRuntime: ").append(convertPath(runtime.getString("path"))).append(";\n");
        }
        Json.Obj testData = objOr(c, "testData");
        if (testData != null) {
            b.append(tab(3)).append("testData:\n").append(EmbeddedDataComposer.compose(testData, tab(4))).append(";\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    private static String diagram(@com.legend.base.Nullable String title, @com.legend.base.Nullable String description, String path) {
        return tab(2) + "{\n"
                + tab(3) + "title: " + convertString(String.valueOf(title), true) + ";\n"
                + (description != null ? tab(3) + "description: " + convertString(description, true) + ";\n" : "")
                + tab(3) + "diagram: " + convertPath(path) + ";\n"
                + tab(2) + "}";
    }

    private static String executable(Json.Obj x) {
        String type = Composing.type(x);
        StringBuilder b = new StringBuilder(tab(2)).append("{\n");
        String id = str(x, "id");
        if ("dataSpacePackageableElementExecutable".equals(type)) {
            if (id != null) {
                b.append(tab(3)).append("id: ").append(id).append(";\n");
            }
            b.append(tab(3)).append("title: ").append(convertString(x.getString("title"), true)).append(";\n");
            optionalString(b, tab(3), "description", str(x, "description"));
            b.append(tab(3)).append("executable: ").append(x.getObj("executable").getString("path")).append(";\n");
        } else if ("dataSpaceTemplateExecutable".equals(type)) {
            b.append(tab(3)).append("id: ").append(id).append(";\n");
            b.append(tab(3)).append("title: ").append(convertString(x.getString("title"), true)).append(";\n");
            optionalString(b, tab(3), "description", str(x, "description"));
            b.append(tab(3)).append("query: ").append(Composing.valueSpecification(x.get("query"))).append(";\n");
        } else {
            throw Composing.refused("no composer rule for a data space executable of _type '" + type + "'");
        }
        optionalString(b, tab(3), "executionContextKey", str(x, "executionContextKey"));
        Json.Obj samples = objOr(x, "sampleValues");
        if (samples != null) {
            b.append(tab(3)).append("sampleValues: Relation\n").append(EmbeddedDataComposer.alignedRelation(samples, tab(4), true)).append(";\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    private static String supportInfo(Json.Obj s) {
        String type = Composing.type(s);
        StringBuilder b = new StringBuilder();
        if ("email".equals(type)) {
            b.append("Email {\n");
            optionalString(b, tab(2), "documentationUrl", str(s, "documentationUrl"));
            b.append(tab(2)).append("address: ").append(convertString(s.getString("address"), true)).append(";\n");
        } else if ("combined".equals(type)) {
            b.append("Combined {\n");
            optionalString(b, tab(2), "documentationUrl", str(s, "documentationUrl"));
            optionalString(b, tab(2), "website", str(s, "website"));
            optionalString(b, tab(2), "faqUrl", str(s, "faqUrl"));
            optionalString(b, tab(2), "supportUrl", str(s, "supportUrl"));
            if (Composing.value(s, "emails") != null) {
                List<String> emails = new ArrayList<>();
                for (String e : s.getStringArrayOr("emails", List.of())) {
                    emails.add(convertString(e, true));
                }
                b.append(tab(2)).append("emails:").append(emails.isEmpty() ? " []"
                        : "\n" + tab(2) + "[\n" + tab(3) + String.join(",\n" + tab(3), emails) + "\n" + tab(2) + "]").append(";\n");
            }
        } else if ("full".equals(type)) {
            b.append("{\n");
            link(b, s, "documentation");
            link(b, s, "website");
            link(b, s, "faqUrl");
            link(b, s, "supportUrl");
            if (Composing.value(s, "emails") != null) {
                List<String> emails = new ArrayList<>();
                for (Json.Obj e : objs(s, "emails")) {
                    emails.add(tab(3) + "{\n" + tab(4) + "title: " + convertString(e.getString("title"), true) + ";\n"
                            + tab(4) + "address: " + convertString(e.getString("address"), true) + ";\n" + tab(3) + "}");
                }
                fullList(b, "emails", emails);
            }
            if (Composing.value(s, "expertise") != null) {
                List<String> expertise = new ArrayList<>();
                for (Json.Obj e : objs(s, "expertise")) {
                    StringBuilder x = new StringBuilder(tab(3)).append("{\n");
                    optionalString(x, tab(4), "description", str(e, "description"));
                    List<String> ids = new ArrayList<>();
                    for (String id : e.getStringArrayOr("expertIds", List.of())) {
                        ids.add(convertString(id, true));
                    }
                    if (!ids.isEmpty()) {
                        x.append(tab(4)).append("expertIds: [").append(String.join(", ", ids)).append("];\n");
                    }
                    expertise.add(x.append(tab(3)).append("}").toString());
                }
                fullList(b, "expertise", expertise);
            }
        } else {
            throw Composing.refused("no composer rule for data space support info of _type '" + type + "'");
        }
        return b.append(TAB).append("}").toString();
    }

    private static void link(StringBuilder b, Json.Obj s, String key) {
        Json.Obj link = objOr(s, key);
        if (link != null) {
            String label = str(link, "label");
            b.append(tab(2)).append(key).append(": { ").append(label != null ? "label: " + convertString(label, true) + "; " : "")
                    .append("url: ").append(convertString(link.getString("url"), true)).append("; };\n");
        }
    }

    private static void fullList(StringBuilder b, String key, List<String> items) {
        b.append(tab(2)).append(key).append(":").append(items.isEmpty() ? " []"
                : "\n" + tab(2) + "[\n" + String.join(",\n", items) + "\n" + tab(2) + "]").append(";\n");
    }

    private static String operationalMetadata(Json.Obj m) {
        StringBuilder b = new StringBuilder("{\n");
        List<String> regions = m.getStringArrayOr("coverageRegions", List.of());
        if (!regions.isEmpty()) {
            b.append(tab(2)).append("coverageRegions: [").append(String.join(", ", regions)).append("];\n");
        }
        String frequency = str(m, "updateFrequency");
        if (frequency != null) {
            b.append(tab(2)).append("updateFrequency: ").append(frequency).append(";\n");
        }
        return b.append(TAB).append("}").toString();
    }
}
