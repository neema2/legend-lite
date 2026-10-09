// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.Protocol.PPersistenceEntry;
import com.legend.protocol.Protocol.PPersistenceNode;
import com.legend.protocol.spec.PathLiteral;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Persistence}'s persistence and persistence context as upstream prints them
 * ({@code PersistenceComposerExtension}, {@code HelperPersistenceComposer}, {@code HelperPersistenceContextComposer}
 * and the cloud and relational extensions' platform and target composers) -- over the records
 * ({@link Protocol.PPersistence}, {@link Protocol.PPersistenceContext}; the protocol program's leg 2, step 3).
 *
 * <p>The persistence sub-DSL is a generic tree ({@link PPersistenceNode}: a grammar kind and entries keyed by the
 * grammar's keys, as {@link PersistenceReader} reads them back): the printers look entries up by key, and pick a
 * node's printer by its kind.
 */
final class PersistenceComposer {

    private PersistenceComposer() {
    }

    /** The section's two kinds: persistences and persistence contexts. */
    static String element(Protocol.Element e) {
        return switch (e) {
            case Protocol.PPersistence p -> persistence(p, 1);
            case Protocol.PPersistenceContext c -> persistenceContext(c, 1);
            default -> throw Composing.refused("no Persistence printer for a " + e.getClass().getSimpleName());
        };
    }

    // ---------------------------------------------------------------------
    // The generic tree
    // ---------------------------------------------------------------------

    /** Looks up a node kind's printer in a table, refusing an unknown one by name. */
    static <T> T rule(Map<String, T> table, PPersistenceNode node, String what) {
        T rule = table.get(node.kind());
        if (rule == null) {
            throw Composing.refused("no composer rule for a persistence " + what + " of kind '" + node.kind() + "'");
        }
        return rule;
    }

    private static @com.legend.base.Nullable PPersistenceEntry entry(PPersistenceNode node, String key) {
        for (PPersistenceEntry e : node.entries()) {
            if (e.key().equals(key)) {
                return e;
            }
        }
        return null;
    }

    private static IllegalArgumentException absent(PPersistenceNode node, String key, String what) {
        return Composing.refused("a persistence " + node.kind() + " without its " + what + " '" + key + "'");
    }

    /** A scalar entry's value, or null when absent. */
    static @com.legend.base.Nullable String optScalar(PPersistenceNode node, String key) {
        return entry(node, key) instanceof PPersistenceEntry.Scalar s ? s.value() : null;
    }

    static String scalar(PPersistenceNode node, String key) {
        String v = optScalar(node, key);
        if (v == null) {
            throw absent(node, key, "value");
        }
        return v;
    }

    static @com.legend.base.Nullable PPersistenceNode optChild(PPersistenceNode node, String key) {
        return entry(node, key) instanceof PPersistenceEntry.Node n ? n.node() : null;
    }

    static PPersistenceNode child(PPersistenceNode node, String key) {
        PPersistenceNode n = optChild(node, key);
        if (n == null) {
            throw absent(node, key, "node");
        }
        return n;
    }

    /** A pointer entry's path. */
    static String pointer(PPersistenceNode node, String key) {
        if (entry(node, key) instanceof PPersistenceEntry.Pointer p) {
            return p.path();
        }
        throw absent(node, key, "pointer");
    }

    /** A list of strings, empty when absent. */
    static List<String> strings(PPersistenceNode node, String key) {
        return entry(node, key) instanceof PPersistenceEntry.Strings s ? s.values() : List.of();
    }

    /** A list of nodes, empty when absent. */
    static List<PPersistenceNode> nodes(PPersistenceNode node, String key) {
        return entry(node, key) instanceof PPersistenceEntry.NodeList l ? l.nodes() : List.of();
    }

    /** A field: a scalar's value, or a path value's text (the graphFetch spelling of the same field). */
    static String field(PPersistenceNode node, String key) {
        return switch (entry(node, key)) {
            case PPersistenceEntry.Scalar s -> s.value();
            case PPersistenceEntry.PathValue p -> path(p.spec());
            case null, default -> throw absent(node, key, "field");
        };
    }

    /** A list of fields: strings, or (the graphFetch spelling) path values, joined by {@code ", "}. */
    static String fields(PPersistenceNode node, String key, String pathKey) {
        if (entry(node, pathKey) instanceof PPersistenceEntry.PathList l) {
            return paths(l.specs());
        }
        return String.join(", ", strings(node, key));
    }

    static String paths(List<ValueSpecification> specs) {
        List<String> out = new ArrayList<>();
        for (ValueSpecification s : specs) {
            out.add(path(s));
        }
        return String.join(", ", out);
    }

    /** {@code ServiceOutputComposer.renderPath}: the start type as written, the properties, the name. */
    static String path(ValueSpecification spec) {
        if (!(spec instanceof PathLiteral path)) {
            throw Composing.refused("a persistence path that is not a path literal");
        }
        List<String> elements = new ArrayList<>();
        for (PathLiteral.Segment s : path.segments()) {
            List<String> args = new ArrayList<>();
            for (PathLiteral.PathArg a : s.args()) {
                args.add(PureComposer.pathArgument(a));
            }
            elements.add(s.name() + (args.size() > 1 ? "(" + String.join(", ", args) + ")" : ""));
        }
        String name = path.alias();
        return "#/" + path.startType() + (elements.isEmpty() ? "" : "/" + String.join("/", elements))
                + (name == null || name.isEmpty() ? "" : "!" + name) + "#";
    }

    // ---------------------------------------------------------------------
    // Persistence
    // ---------------------------------------------------------------------

    private static String persistence(Protocol.PPersistence p, int i) {
        if (p.doc() == null) {
            throw Composing.refused("a persistence with no documentation (upstream cannot print it)");
        }
        if (!"Manual".equals(p.triggerKind())) {
            // upstream composes the manual trigger only
            throw Composing.refused("no composer rule for a persistence trigger of kind '" + p.triggerKind() + "'");
        }
        if (p.service() == null) {
            throw Composing.refused("a persistence with no service");
        }
        return "Persistence " + Composing.elementPath(p.pkg(), p.name()) + "\n{\n"
                + tab(i) + "doc: " + convertString(p.doc(), true) + ";\n"
                + tab(i) + "trigger: Manual;\n"
                + tab(i) + "service: " + p.service() + ";\n"
                + serviceOutputTargets(p.serviceOutputTargets(), i)
                + (p.persister() == null ? "" : PersistencePersisterComposer.persister(p.persister(), i))
                + notifier(p.notifier(), i)
                + tests(p.tests(), i)
                + "}";
    }

    private static String serviceOutputTargets(@com.legend.base.Nullable List<Protocol.PServiceOutputTarget> targets, int i) {
        if (targets == null || targets.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PServiceOutputTarget t : targets) {
            out.add(PersistenceOutputComposer.serviceOutput(t.serviceOutput(), i + 1)
                    + tab(i + 1) + "->\n" + PersistenceOutputComposer.target(t.persistenceTarget(), i + 1));
        }
        return tab(i) + "serviceOutputTargets:\n" + tab(i) + "[\n" + String.join(",\n", out) + "\n" + tab(i) + "];\n";
    }

    private static String notifier(Protocol.@com.legend.base.Nullable PPersistenceNotifier notifier, int i) {
        if (notifier == null || notifier.notifyees().isEmpty()) {
            return "";
        }
        int n = i + 2;
        List<String> out = new ArrayList<>();
        for (PPersistenceNode e : notifier.notifyees()) {
            out.add(switch (e.kind()) {
                case "Email" -> tab(n) + "Email\n" + tab(n) + "{\n" + tab(n + 1) + "address: '" + scalar(e, "address") + "';\n" + tab(n) + "}";
                case "PagerDuty" -> tab(n) + "PagerDuty\n" + tab(n) + "{\n" + tab(n + 1) + "url: '" + scalar(e, "url") + "';\n" + tab(n) + "}";
                default -> throw Composing.refused("no composer rule for a persistence notifyee of kind '" + e.kind() + "'");
            });
        }
        return tab(i) + "notifier:\n" + tab(i) + "{\n"
                + tab(i + 1) + "notifyees:\n" + tab(i + 1) + "[\n" + String.join(",\n", out) + "\n" + tab(i + 1) + "];\n"
                + tab(i) + "}\n";
    }

    private static String tests(@com.legend.base.Nullable List<Protocol.PPersistenceTest> tests, int i) {
        if (tests == null) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PPersistenceTest t : tests) {
            out.add(stripTrailingWhitespace(test(t, i + 1)));
        }
        return tab(i) + "tests:\n" + tab(i) + "[\n" + String.join(",\n", out) + "\n" + tab(i) + "]\n";
    }

    /** {@code replaceAll("\\s+$", "")}. */
    private static String stripTrailingWhitespace(String s) {
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end);
    }

    private static String test(Protocol.PPersistenceTest t, int i) {
        List<String> bs = new ArrayList<>();
        for (Protocol.PPersistenceTestBatch b : t.testBatches()) {
            bs.add(testBatch(b, i + 2));
        }
        String batches = "testBatches:\n" + tab(i + 1) + "[\n" + String.join(",\n", bs) + "\n" + tab(i + 1) + "]\n";
        String isFromOutput = "isTestDataFromServiceOutput: " + t.isTestDataFromServiceOutput() + ";\n";
        String path = t.graphFetchPath() == null ? "" : tab(i + 1) + "graphFetchPath: " + path(t.graphFetchPath()) + ";\n";
        return tab(i) + t.id() + ":\n" + tab(i) + "{\n"
                + tab(i + 1) + batches + tab(i + 1) + isFromOutput + path + tab(i) + "}\n";
    }

    private static String testBatch(Protocol.PPersistenceTestBatch b, int i) {
        StringBuilder s = new StringBuilder(tab(i)).append(b.id()).append(":\n").append(tab(i)).append("{\n");
        s.append(tab(i + 1)).append("data:\n").append(tab(i + 1)).append("{\n")
                .append(tab(i + 2)).append("connection:\n").append(tab(i + 2)).append("{\n")
                .append(EmbeddedDataComposer.compose(externalFormat(b.connectionData()), tab(i + 3))).append("\n")
                .append(tab(i + 2)).append("}\n")
                .append(tab(i + 1)).append("}\n");
        List<String> as = new ArrayList<>();
        for (Protocol.PPersistenceAssert a : b.asserts()) {
            as.add(TestAssertionComposer.compose(assertion(a), tab(i + 2)));
        }
        s.append(tab(i + 1)).append("asserts:\n").append(tab(i + 1)).append("[\n").append(String.join(",\n", as)).append("\n")
                .append(tab(i + 1)).append("]\n");
        return s.append(tab(i)).append("}").toString();
    }

    /** A test's connection data: the reader knows the external format kind only. */
    private static Protocol.PExternalFormatData externalFormat(PPersistenceNode data) {
        if (!"ExternalFormat".equals(data.kind())) {
            throw Composing.refused("no composer rule for persistence test data of kind '" + data.kind() + "'");
        }
        return new Protocol.PExternalFormatData(scalar(data, "contentType"), scalar(data, "data"), null);
    }

    /** A test's assertion: an equal-to-JSON over its expected external format data. */
    private static Protocol.PTestAssertion assertion(Protocol.PPersistenceAssert a) {
        if (!"EqualToJson".equals(a.assertion().kind())) {
            throw Composing.refused("no composer rule for a persistence assertion of kind '" + a.assertion().kind() + "'");
        }
        return new Protocol.PTestAssertion(a.id(), externalFormat(child(a.assertion(), "expected")), null);
    }

    // ---------------------------------------------------------------------
    // PersistenceContext
    // ---------------------------------------------------------------------

    private static String persistenceContext(Protocol.PPersistenceContext c, int i) {
        String platform = platform(c.platform(), i);
        StringBuilder b = new StringBuilder("PersistenceContext ").append(Composing.elementPath(c.pkg(), c.name())).append("\n{\n")
                .append(tab(i)).append("persistence: ").append(convertPath(c.persistence())).append(";\n")
                .append(platform.isEmpty() ? "" : tab(i) + "platform: " + platform + ";\n");
        if (!c.serviceParameters().isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Protocol.PCtxParam p : c.serviceParameters()) {
                ps.add(tab(i + 1) + p.name() + "=" + serviceParameterValue(p.value(), i + 1));
            }
            b.append(tab(i)).append("serviceParameters:\n").append(tab(i)).append("[\n").append(String.join(",\n", ps)).append("\n")
                    .append(tab(i)).append("];\n");
        }
        if (c.sinkConnection() != null) {
            b.append(connection(c.sinkConnection(), "sinkConnection", i));
        }
        return b.append("}").toString();
    }

    /** The default platform (unspelled, or spelled bare) prints nothing; AWS Glue its block. */
    private static String platform(@com.legend.base.Nullable PPersistenceNode platform, int i) {
        if (platform == null || "Default".equals(platform.kind())) {
            return "";
        }
        if (!"AwsGlue".equals(platform.kind())) {
            throw Composing.refused("no composer rule for a persistence platform of kind '" + platform.kind() + "'");
        }
        return "AwsGlue\n" + tab(i) + "#{\n" + tab(i + 1) + "dataProcessingUnits: " + scalar(platform, "dataProcessingUnits")
                + ";\n" + tab(i) + "}#";
    }

    private static String serviceParameterValue(Protocol.PCtxParamValue value, int i) {
        return switch (value) {
            case Protocol.PCtxParamValue.Primitive p -> Composing.valueSpecification(p.spec());
            case Protocol.PCtxParamValue.ConnectionPtr p -> convertPath(p.path());
            case Protocol.PCtxParamValue.ConnectionVal v -> connection(v.connection(), null, i);
        };
    }

    /** {@code renderConnection}: a pointer by its path, a value embedded in {@code #{ }#}. */
    private static String connection(Protocol.PConnectionValue connection, @com.legend.base.Nullable String prefix, int i) {
        if (connection instanceof Protocol.PConnectionPointer p) {
            return (prefix == null ? "" : tab(i) + prefix + ": ") + convertPath(p.connection()) + (prefix == null ? "" : ";\n");
        }
        return (prefix == null ? "\n" : tab(i) + prefix + ":\n")
                + tab(i) + "#{\n"
                + tab(i + 1) + ConnectionComposer.keyword(connection) + "\n"
                + ConnectionComposer.body(connection, tab(i + 1)) + "\n"
                + tab(i) + "}#" + (prefix == null ? "" : ";\n");
    }
}
