// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###DataSpace}'s data space as upstream prints it ({@code DataSpaceGrammarComposerExtension.renderDataSpace})
 * -- over the record ({@link Protocol.PDataSpace}; the protocol program's leg 2, step 3).
 */
final class DataSpaceComposer {

    private DataSpaceComposer() {
    }

    static String dataSpace(Protocol.PDataSpace d) {
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("DataSpace", "", d.stereotypes(), d.taggedValues()))
                .append(Composing.elementPath(d.pkg(), d.name())).append("\n{\n");
        List<Protocol.PDataSpaceContext> contexts = d.executionContexts() == null ? List.of() : d.executionContexts();
        if (!contexts.isEmpty()) {
            List<String> cs = new ArrayList<>();
            for (Protocol.PDataSpaceContext c : contexts) {
                cs.add(executionContext(c));
            }
            b.append(TAB).append("executionContexts:\n").append(TAB).append("[\n").append(String.join(",\n", cs)).append("\n").append(TAB).append("];\n");
        }
        optionalString(b, TAB, "defaultExecutionContext", d.defaultExecutionContext());
        optionalString(b, TAB, "title", d.title());
        optionalString(b, TAB, "description", d.description());
        if (d.diagrams() != null) {
            List<String> diagrams = new ArrayList<>();
            for (Protocol.PDataSpaceDiagram g : d.diagrams()) {
                diagrams.add(diagram(g));
            }
            list(b, "diagrams", diagrams);
        }
        if (d.elements() != null) {
            List<String> es = new ArrayList<>();
            for (Protocol.PDataSpaceElementRef e : d.elements()) {
                es.add((e.exclude() ? "-" : "") + e.path());
            }
            b.append(TAB).append("elements:").append(es.isEmpty() ? " []"
                    : "\n" + TAB + "[\n" + tab(2) + String.join(",\n" + tab(2), es) + "\n" + TAB + "]").append(";\n");
        }
        if (d.executables() != null) {
            List<String> xs = new ArrayList<>();
            for (Protocol.PDataSpaceExecutable x : d.executables()) {
                xs.add(executable(x));
            }
            list(b, "executables", xs);
        }
        if (d.supportInfo() != null) {
            b.append(TAB).append("supportInfo: ").append(supportInfo(d.supportInfo())).append(";\n");
        }
        if (d.operationalMetadata() != null) {
            b.append(TAB).append("operationalMetadata: ").append(operationalMetadata(d.operationalMetadata())).append(";\n");
        }
        return b.append("}").toString();
    }

    /** {@link #dataSpace(Protocol.PDataSpace)} of the JSON, read first. */
    static String dataSpace(Json.Obj d) {
        return dataSpace(Composing.element(d, Protocol.PDataSpace.class));
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

    private static String executionContext(Protocol.PDataSpaceContext c) {
        StringBuilder b = new StringBuilder(tab(2)).append("{\n");
        b.append(tab(3)).append("name: ").append(convertString(c.name(), true)).append(";\n");
        optionalString(b, tab(3), "title", c.title());
        optionalString(b, tab(3), "description", c.description());
        if (c.mapping() != null) {
            b.append(tab(3)).append("mapping: ").append(convertPath(c.mapping())).append(";\n");
        }
        Protocol.PDataSpaceMappingProvider provider = c.mappingProvider();
        if (provider != null) {
            b.append(tab(3)).append("mappingProvider: ").append(convertPath(provider.element()))
                    .append(provider.keys().isEmpty() ? "" : "." + String.join(",", provider.keys())).append(";\n");
        }
        if (c.defaultRuntime() != null) {
            b.append(tab(3)).append("defaultRuntime: ").append(convertPath(c.defaultRuntime())).append(";\n");
        }
        Protocol.PDataSpaceTestData testData = c.testData();
        if (testData != null) {
            // a reference to a data element, or (DataspaceTestData) to a data space's test data
            String type = "DataspaceTestData".equals(testData.kind()) ? "DATASPACE" : "DATA";
            Protocol.PDataReference reference = new Protocol.PDataReference(
                    new Protocol.PPointer(type, testData.path(), null), null);
            b.append(tab(3)).append("testData:\n").append(EmbeddedDataComposer.compose(reference, tab(4))).append(";\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    private static String diagram(Protocol.PDataSpaceDiagram g) {
        return tab(2) + "{\n"
                + tab(3) + "title: " + convertString(g.title(), true) + ";\n"
                + (g.description() != null ? tab(3) + "description: " + convertString(g.description(), true) + ";\n" : "")
                + tab(3) + "diagram: " + convertPath(g.diagram()) + ";\n"
                + tab(2) + "}";
    }

    /** An element executable carries its pointer; a template its query (and always prints its id). */
    private static String executable(Protocol.PDataSpaceExecutable x) {
        StringBuilder b = new StringBuilder(tab(2)).append("{\n");
        if (x.query() == null) {
            if (x.id() != null) {
                b.append(tab(3)).append("id: ").append(x.id()).append(";\n");
            }
            b.append(tab(3)).append("title: ").append(convertString(x.title(), true)).append(";\n");
            optionalString(b, tab(3), "description", x.description());
            b.append(tab(3)).append("executable: ").append(x.executable()).append(";\n");
        } else {
            b.append(tab(3)).append("id: ").append(x.id()).append(";\n");
            b.append(tab(3)).append("title: ").append(convertString(x.title(), true)).append(";\n");
            optionalString(b, tab(3), "description", x.description());
            b.append(tab(3)).append("query: ").append(Composing.valueSpecification(x.query())).append(";\n");
        }
        optionalString(b, tab(3), "executionContextKey", x.executionContextKey());
        if (x.sampleValues() != null) {
            b.append(tab(3)).append("sampleValues: Relation\n")
                    .append(EmbeddedDataComposer.alignedRelation(x.sampleValues(), tab(4), true)).append(";\n");
        }
        return b.append(tab(2)).append("}").toString();
    }

    private static String supportInfo(Protocol.PDataSpaceSupport s) {
        StringBuilder b = new StringBuilder();
        switch (s) {
            case Protocol.PDataSpaceSupport.PSupportEmail e -> {
                b.append("Email {\n");
                optionalString(b, tab(2), "documentationUrl", e.documentationUrl());
                b.append(tab(2)).append("address: ").append(convertString(e.address(), true)).append(";\n");
            }
            case Protocol.PDataSpaceSupport.PSupportCombined c -> {
                b.append("Combined {\n");
                optionalString(b, tab(2), "documentationUrl", c.documentationUrl());
                optionalString(b, tab(2), "website", c.website());
                optionalString(b, tab(2), "faqUrl", c.faqUrl());
                optionalString(b, tab(2), "supportUrl", c.supportUrl());
                if (c.emails() != null) {
                    List<String> emails = new ArrayList<>();
                    for (String e : c.emails()) {
                        emails.add(convertString(e, true));
                    }
                    b.append(tab(2)).append("emails:").append(emails.isEmpty() ? " []"
                            : "\n" + tab(2) + "[\n" + tab(3) + String.join(",\n" + tab(3), emails) + "\n" + tab(2) + "]").append(";\n");
                }
            }
            case Protocol.PDataSpaceSupport.PSupportFull f -> {
                b.append("{\n");
                link(b, "documentation", f.documentation());
                link(b, "website", f.website());
                link(b, "faqUrl", f.faqUrl());
                link(b, "supportUrl", f.supportUrl());
                if (f.emails() != null) {
                    List<String> emails = new ArrayList<>();
                    for (Protocol.PDataSpaceEmail e : f.emails()) {
                        emails.add(tab(3) + "{\n" + tab(4) + "title: " + convertString(e.title(), true) + ";\n"
                                + tab(4) + "address: " + convertString(e.address(), true) + ";\n" + tab(3) + "}");
                    }
                    fullList(b, "emails", emails);
                }
                if (f.expertise() != null) {
                    List<String> expertise = new ArrayList<>();
                    for (Protocol.PDataSpaceExpertise e : f.expertise()) {
                        StringBuilder x = new StringBuilder(tab(3)).append("{\n");
                        optionalString(x, tab(4), "description", e.description());
                        List<String> ids = new ArrayList<>();
                        for (String id : e.expertIds() == null ? List.<String>of() : e.expertIds()) {
                            ids.add(convertString(id, true));
                        }
                        if (!ids.isEmpty()) {
                            x.append(tab(4)).append("expertIds: [").append(String.join(", ", ids)).append("];\n");
                        }
                        expertise.add(x.append(tab(3)).append("}").toString());
                    }
                    fullList(b, "expertise", expertise);
                }
            }
        }
        return b.append(TAB).append("}").toString();
    }

    private static void link(StringBuilder b, String key, Protocol.@com.legend.base.Nullable PDataSpaceLink link) {
        if (link != null) {
            b.append(tab(2)).append(key).append(": { ").append(link.label() != null ? "label: " + convertString(link.label(), true) + "; " : "")
                    .append("url: ").append(convertString(link.url(), true)).append("; };\n");
        }
    }

    private static void fullList(StringBuilder b, String key, List<String> items) {
        b.append(tab(2)).append(key).append(":").append(items.isEmpty() ? " []"
                : "\n" + tab(2) + "[\n" + String.join(",\n", items) + "\n" + tab(2) + "]").append(";\n");
    }

    private static String operationalMetadata(Protocol.PDataSpaceOperationalMetadata m) {
        StringBuilder b = new StringBuilder("{\n");
        if (!m.coverageRegions().isEmpty()) {
            b.append(tab(2)).append("coverageRegions: [").append(String.join(", ", m.coverageRegions())).append("];\n");
        }
        if (m.updateFrequency() != null) {
            b.append(tab(2)).append("updateFrequency: ").append(m.updateFrequency()).append(";\n");
        }
        return b.append(TAB).append("}").toString();
    }
}
