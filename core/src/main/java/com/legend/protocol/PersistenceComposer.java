// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Persistence}'s persistence and persistence context as upstream prints them
 * ({@code PersistenceComposerExtension}, {@code HelperPersistenceComposer}, {@code HelperPersistenceContextComposer}
 * and the cloud and relational extensions' platform and target composers).
 */
final class PersistenceComposer {

    private static final String PERSISTENCE_CONTEXT = "persistenceContext";

    /** The persistence platforms, by {@code _type}: the default one prints nothing. */
    private static final Map<String, BiFunction<Json.Obj, Integer, String>> PLATFORMS = Map.of(
            "awsGlue", (p, i) -> "AwsGlue\n" + tab(i) + "#{\n" + tab(i + 1) + "dataProcessingUnits: "
                    + RelationalConnectionComposer.raw(p.get("dataProcessingUnits")) + ";\n" + tab(i) + "}#",
            "default", (p, i) -> "");

    /** The triggers, by {@code _type}: upstream composes the manual trigger only. */
    private static final Map<String, String> TRIGGERS = Map.of("manualTrigger", "Manual");

    private PersistenceComposer() {
    }

    /** The section's two kinds: persistences and persistence contexts. */
    static String element(Json.Obj e) {
        return PERSISTENCE_CONTEXT.equals(Composing.type(e)) ? persistenceContext(e, 1) : persistence(e, 1);
    }

    /** Looks up a {@code _type}'s printer in a table, refusing an unknown one by name. */
    static <T> T rule(Map<String, T> table, Json.Obj o, String what) {
        T rule = table.get(Composing.type(o));
        if (rule == null) {
            throw Composing.refused("no composer rule for a persistence " + what + " of _type '" + Composing.type(o) + "'");
        }
        return rule;
    }

    // ---------------------------------------------------------------------
    // Persistence
    // ---------------------------------------------------------------------

    private static String persistence(Json.Obj p, int i) {
        String documentation = str(p, "documentation");
        if (documentation == null) {
            throw Composing.refused("a persistence with no documentation (upstream cannot print it)");
        }
        return "Persistence " + elementPath(p) + "\n{\n"
                + tab(i) + "doc: " + convertString(documentation, true) + ";\n"
                + tab(i) + "trigger: " + rule(TRIGGERS, p.getObj("trigger"), "trigger") + ";\n"
                + tab(i) + "service: " + p.getObj("service").getString("path") + ";\n"
                + serviceOutputTargets(objs(p, "serviceOutputTargets"), i)
                + persister(objOr(p, "persister"), i)
                + notifier(p.getObj("notifier"), i)
                + tests(p, i)
                + "}";
    }

    private static String serviceOutputTargets(List<Json.Obj> targets, int i) {
        if (targets.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Json.Obj t : targets) {
            out.add(PersistenceOutputComposer.serviceOutput(t.getObj("serviceOutput"), i + 1)
                    + tab(i + 1) + "->\n" + PersistenceOutputComposer.target(objOr(t, "persistenceTarget"), i + 1));
        }
        return tab(i) + "serviceOutputTargets:\n" + tab(i) + "[\n" + String.join(",\n", out) + "\n" + tab(i) + "];\n";
    }

    private static String persister(@com.legend.base.Nullable Json.Obj persister, int i) {
        return persister == null ? "" : PersistencePersisterComposer.persister(persister, i);
    }

    private static String notifier(Json.Obj notifier, int i) {
        List<Json.Obj> notifyees = objs(notifier, "notifyees");
        if (notifyees.isEmpty()) {
            return "";
        }
        int n = i + 2;
        List<String> out = new ArrayList<>();
        for (Json.Obj e : notifyees) {
            String type = Composing.type(e);
            if ("emailNotifyee".equals(type)) {
                out.add(tab(n) + "Email\n" + tab(n) + "{\n" + tab(n + 1) + "address: '" + e.getString("address") + "';\n" + tab(n) + "}");
            } else if ("pagerDutyNotifyee".equals(type)) {
                out.add(tab(n) + "PagerDuty\n" + tab(n) + "{\n" + tab(n + 1) + "url: '" + e.getString("url") + "';\n" + tab(n) + "}");
            } else {
                throw Composing.refused("no composer rule for a persistence notifyee of _type '" + type + "'");
            }
        }
        return tab(i) + "notifier:\n" + tab(i) + "{\n"
                + tab(i + 1) + "notifyees:\n" + tab(i + 1) + "[\n" + String.join(",\n", out) + "\n" + tab(i + 1) + "];\n"
                + tab(i) + "}\n";
    }

    private static String tests(Json.Obj p, int i) {
        if (Composing.value(p, "tests") == null) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Json.Obj t : objs(p, "tests")) {
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

    private static String test(Json.Obj t, int i) {
        StringBuilder batches = new StringBuilder();
        if (Composing.value(t, "testBatches") != null) {
            List<String> bs = new ArrayList<>();
            for (Json.Obj b : objs(t, "testBatches")) {
                bs.add(testBatch(b, i + 2));
            }
            batches.append("testBatches:\n").append(tab(i + 1)).append("[\n").append(String.join(",\n", bs)).append("\n")
                    .append(tab(i + 1)).append("]\n");
        }
        Json.Node fromOutput = Composing.value(t, "isTestDataFromServiceOutput");
        String isFromOutput = fromOutput == null ? "" : "isTestDataFromServiceOutput: " + RelationalConnectionComposer.raw(fromOutput) + ";\n";
        Json.Obj graphFetchPath = objOr(t, "graphFetchPath");
        String path = graphFetchPath == null ? "" : tab(i + 1) + "graphFetchPath: " + PersistenceOutputComposer.path(graphFetchPath) + ";\n";
        return tab(i) + t.getString("id") + ":\n" + tab(i) + "{\n"
                + tab(i + 1) + batches + tab(i + 1) + isFromOutput + path + tab(i) + "}\n";
    }

    private static String testBatch(Json.Obj b, int i) {
        StringBuilder s = new StringBuilder(tab(i)).append(b.getString("id")).append(":\n").append(tab(i)).append("{\n");
        Json.Obj data = objOr(b, "testData");
        if (data != null) {
            s.append(tab(i + 1)).append("data:\n").append(tab(i + 1)).append("{\n");
            Json.Obj connection = objOr(data, "connection");
            if (connection != null) {
                s.append(tab(i + 2)).append("connection:\n").append(tab(i + 2)).append("{\n")
                        .append(EmbeddedDataComposer.compose(connection.getObj("data"), tab(i + 3))).append("\n")
                        .append(tab(i + 2)).append("}\n");
            }
            s.append(tab(i + 1)).append("}\n");
        }
        if (Composing.value(b, "assertions") != null) {
            List<String> as = new ArrayList<>();
            for (Json.Obj a : objs(b, "assertions")) {
                as.add(TestAssertionComposer.compose(a, tab(i + 2)));
            }
            s.append(tab(i + 1)).append("asserts:\n").append(tab(i + 1)).append("[\n").append(String.join(",\n", as)).append("\n")
                    .append(tab(i + 1)).append("]\n");
        }
        return s.append(tab(i)).append("}").toString();
    }

    // ---------------------------------------------------------------------
    // PersistenceContext
    // ---------------------------------------------------------------------

    private static String persistenceContext(Json.Obj c, int i) {
        String platform = rule(PLATFORMS, c.getObj("platform"), "platform").apply(c.getObj("platform"), i);
        List<Json.Obj> parameters = objs(c, "serviceParameters");
        StringBuilder b = new StringBuilder("PersistenceContext ").append(elementPath(c)).append("\n{\n")
                .append(tab(i)).append("persistence: ").append(convertPath(c.getObj("persistence").getString("path"))).append(";\n")
                .append(platform.isEmpty() ? "" : tab(i) + "platform: " + platform + ";\n");
        if (!parameters.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Obj p : parameters) {
                ps.add(tab(i + 1) + p.getString("name") + "=" + serviceParameterValue(p.getObj("value"), i + 1));
            }
            b.append(tab(i)).append("serviceParameters:\n").append(tab(i)).append("[\n").append(String.join(",\n", ps)).append("\n")
                    .append(tab(i)).append("];\n");
        }
        Json.Obj sink = objOr(c, "sinkConnection");
        if (sink != null) {
            b.append(connection(sink, "sinkConnection", i));
        }
        return b.append("}").toString();
    }

    private static String serviceParameterValue(Json.Obj value, int i) {
        String type = Composing.type(value);
        if ("primitiveTypeValue".equals(type)) {
            return Composing.valueSpecification(value.get("primitiveType"));
        }
        if ("connectionValue".equals(type)) {
            return connection(value.getObj("connection"), null, i);
        }
        throw Composing.refused("no composer rule for a persistence service parameter value of _type '" + type + "'");
    }

    /** {@code renderConnection}: a pointer by its path, a value embedded in {@code #{ }#}. */
    private static String connection(Json.Obj connection, @com.legend.base.Nullable String prefix, int i) {
        if ("connectionPointer".equals(Composing.type(connection))) {
            return (prefix == null ? "" : tab(i) + prefix + ": ") + convertPath(connection.getString("connection")) + (prefix == null ? "" : ";\n");
        }
        Protocol.PConnectionValue value = ConnectionReader.connectionValue(connection);
        return (prefix == null ? "\n" : tab(i) + prefix + ":\n")
                + tab(i) + "#{\n"
                + tab(i + 1) + ConnectionComposer.keyword(value) + "\n"
                + ConnectionComposer.body(value, tab(i + 1)) + "\n"
                + tab(i) + "}#" + (prefix == null ? "" : ";\n");
    }

    /** A list of strings joined as upstream's {@code makeString(", ")} does. */
    static String joined(Json.Obj o, String key) {
        return String.join(", ", o.getStringArrayOr(key, List.of()));
    }
}
