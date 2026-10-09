// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.Protocol.PPersistenceNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.PersistenceComposer.child;
import static com.legend.protocol.PersistenceComposer.nodes;
import static com.legend.protocol.PersistenceComposer.optChild;
import static com.legend.protocol.PersistenceComposer.optScalar;
import static com.legend.protocol.PersistenceComposer.pointer;
import static com.legend.protocol.PersistenceComposer.rule;
import static com.legend.protocol.PersistenceComposer.scalar;
import static com.legend.protocol.PersistenceComposer.strings;

/**
 * A persistence's (v1) persister as upstream prints it ({@code HelperPersistenceComposer}'s persister, sink,
 * target shape, deduplication strategy, ingest mode, auditing, milestoning and merge strategy visitors) -- over the
 * persistence tree ({@link PPersistenceNode}; the protocol program's leg 2, step 3).
 */
final class PersistencePersisterComposer {

    /** A printer of one kind of node at an indentation level. */
    private interface Printer extends BiFunction<PPersistenceNode, Integer, String> {
    }

    private static final Map<String, Printer> PERSISTERS = Map.of(
            "Streaming", (p, i) -> tab(i) + "persister: Streaming\n" + tab(i) + "{\n" + sink(p, i + 1) + tab(i) + "}\n",
            "Batch", (p, i) -> tab(i) + "persister: Batch\n" + tab(i) + "{\n" + sink(p, i + 1)
                    + apply(PersistencePersisterComposer.INGEST_MODES, child(p, "ingestMode"), i + 1, "ingest mode")
                    + apply(PersistencePersisterComposer.TARGET_SHAPES, child(p, "targetShape"), i + 1, "target shape") + tab(i) + "}\n");

    private static final Map<String, Printer> SINKS = Map.of(
            "Relational", (s, i) -> tab(i) + "sink: Relational\n" + tab(i) + "{\n"
                    + tab(i + 1) + "database: " + pointer(s, "database") + ";\n" + tab(i) + "}\n",
            "ObjectStorage", (s, i) -> tab(i) + "sink: ObjectStorage\n" + tab(i) + "{\n"
                    + tab(i + 1) + "binding: " + pointer(s, "binding") + ";\n" + tab(i) + "}\n");

    private static final Map<String, Printer> TARGET_SHAPES = Map.of(
            "Flat", PersistencePersisterComposer::flatTarget,
            "MultiFlat", PersistencePersisterComposer::multiFlatTarget);

    private static final Map<String, Printer> DEDUPLICATION_STRATEGIES = Map.of(
            "None", (d, i) -> "",
            "AnyVersion", (d, i) -> tab(i) + "deduplicationStrategy: AnyVersion;\n",
            "MaxVersion", (d, i) -> tab(i) + "deduplicationStrategy: MaxVersion\n" + tab(i) + "{\n"
                    + tab(i + 1) + "versionField: " + scalar(d, "versionField") + ";\n" + tab(i) + "}\n",
            "DuplicateCount", (d, i) -> tab(i) + "deduplicationStrategy: DuplicateCount\n" + tab(i) + "{\n"
                    + tab(i + 1) + "duplicateCountName: '" + scalar(d, "duplicateCountName") + "';\n" + tab(i) + "}\n");

    private static final Map<String, Printer> INGEST_MODES = Map.of(
            "NontemporalSnapshot", (m, i) -> ingestMode("NontemporalSnapshot", i, auditing(m, i + 1)),
            "UnitemporalSnapshot", (m, i) -> ingestMode("UnitemporalSnapshot", i, transaction(m, i + 1)),
            "BitemporalSnapshot", (m, i) -> ingestMode("BitemporalSnapshot", i, transaction(m, i + 1) + validity(m, i + 1)),
            "NontemporalDelta", (m, i) -> ingestMode("NontemporalDelta", i, merge(m, i + 1) + auditing(m, i + 1)),
            "UnitemporalDelta", (m, i) -> ingestMode("UnitemporalDelta", i, merge(m, i + 1) + transaction(m, i + 1)),
            "BitemporalDelta", (m, i) -> ingestMode("BitemporalDelta", i, merge(m, i + 1) + transaction(m, i + 1) + validity(m, i + 1)),
            "AppendOnly", (m, i) -> ingestMode("AppendOnly", i, auditing(m, i + 1)
                    + tab(i + 1) + "filterDuplicates: " + scalar(m, "filterDuplicates") + ";\n"));

    private static final Map<String, Printer> AUDITINGS = Map.of(
            "None", (a, i) -> tab(i) + "auditing: None;\n",
            "DateTime", (a, i) -> tab(i) + "auditing: DateTime\n" + tab(i) + "{\n"
                    + tab(i + 1) + "dateTimeName: '" + scalar(a, "dateTimeName") + "';\n" + tab(i) + "}\n");

    private static final Map<String, Printer> TRANSACTION_MILESTONINGS = Map.of(
            "BatchId", (t, i) -> milestoning("transactionMilestoning: BatchId", i,
                    quoted(t, "batchIdInName", i) + quoted(t, "batchIdOutName", i)),
            "DateTime", (t, i) -> milestoning("transactionMilestoning: DateTime", i,
                    quoted(t, "dateTimeInName", i) + quoted(t, "dateTimeOutName", i)
                            + optional(PersistencePersisterComposer.TRANSACTION_DERIVATIONS, optChild(t, "derivation"), i + 1)),
            "BatchIdAndDateTime", (t, i) -> milestoning("transactionMilestoning: BatchIdAndDateTime", i,
                    quoted(t, "batchIdInName", i) + quoted(t, "batchIdOutName", i) + quoted(t, "dateTimeInName", i)
                            + quoted(t, "dateTimeOutName", i)
                            + optional(PersistencePersisterComposer.TRANSACTION_DERIVATIONS, optChild(t, "derivation"), i + 1)));

    private static final Map<String, Printer> TRANSACTION_DERIVATIONS = Map.of(
            "SourceSpecifiesInDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesInDateTime", i,
                    bare(d, "sourceDateTimeInField", i)),
            "SourceSpecifiesInAndOutDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesInAndOutDateTime", i,
                    bare(d, "sourceDateTimeInField", i) + bare(d, "sourceDateTimeOutField", i)));

    private static final Map<String, Printer> VALIDITY_MILESTONINGS = Map.of(
            "DateTime", (v, i) -> milestoning("validityMilestoning: DateTime", i,
                    quoted(v, "dateTimeFromName", i) + quoted(v, "dateTimeThruName", i)
                            + apply(PersistencePersisterComposer.VALIDITY_DERIVATIONS, child(v, "derivation"), i + 1, "validity derivation")));

    private static final Map<String, Printer> VALIDITY_DERIVATIONS = Map.of(
            "SourceSpecifiesFromDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesFromDateTime", i,
                    bare(d, "sourceDateTimeFromField", i)),
            "SourceSpecifiesFromAndThruDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesFromAndThruDateTime", i,
                    bare(d, "sourceDateTimeFromField", i) + bare(d, "sourceDateTimeThruField", i)));

    private static final Map<String, Printer> MERGE_STRATEGIES = Map.of(
            "NoDeletes", (s, i) -> tab(i) + "mergeStrategy: NoDeletes;\n",
            "DeleteIndicator", PersistencePersisterComposer::deleteIndicatorMerge);

    private PersistencePersisterComposer() {
    }

    static String persister(PPersistenceNode persister, int i) {
        return apply(PERSISTERS, persister, i, "persister");
    }

    private static String apply(Map<String, Printer> table, PPersistenceNode o, int i, String what) {
        return rule(table, o, what).apply(o, i);
    }

    private static String optional(Map<String, Printer> table, @com.legend.base.Nullable PPersistenceNode o, int i) {
        return o == null ? "" : apply(table, o, i, "derivation");
    }

    private static String sink(PPersistenceNode persister, int i) {
        return apply(SINKS, child(persister, "sink"), i, "sink");
    }

    private static String flatTarget(PPersistenceNode t, int i) {
        String modelClass = optScalar(t, "modelClass");
        return tab(i) + "targetShape: Flat\n" + tab(i) + "{\n"
                + (modelClass == null ? "" : tab(i + 1) + "modelClass: " + modelClass + ";\n")
                + tab(i + 1) + "targetName: " + convertString(scalar(t, "targetName"), true) + ";\n"
                + partitionFields(t, i + 1)
                + apply(DEDUPLICATION_STRATEGIES, child(t, "deduplicationStrategy"), i + 1, "deduplication strategy")
                + tab(i) + "}\n";
    }

    private static String multiFlatTarget(PPersistenceNode t, int i) {
        List<PPersistenceNode> parts = nodes(t, "parts");
        StringBuilder b = new StringBuilder();
        for (int p = 0; p < parts.size(); p++) {
            PPersistenceNode part = parts.get(p);
            b.append(tab(i + 2)).append("{\n")
                    .append(tab(i + 3)).append("modelProperty: ").append(scalar(part, "modelProperty")).append(";\n")
                    .append(tab(i + 3)).append("targetName: ").append(convertString(scalar(part, "targetName"), true)).append(";\n")
                    .append(partitionFields(part, i + 3))
                    .append(apply(DEDUPLICATION_STRATEGIES, child(part, "deduplicationStrategy"), i + 3, "deduplication strategy"))
                    .append(tab(i + 2)).append(p < parts.size() - 1 ? "},\n" : "}\n");
        }
        return tab(i) + "targetShape: MultiFlat\n" + tab(i) + "{\n"
                + tab(i + 1) + "modelClass: " + scalar(t, "modelClass") + ";\n"
                + tab(i + 1) + "transactionScope: " + scalar(t, "transactionScope") + ";\n"
                + tab(i + 1) + "parts:\n" + tab(i + 1) + "[\n" + b + tab(i + 1) + "];\n"
                + tab(i) + "}\n";
    }

    private static String partitionFields(PPersistenceNode o, int i) {
        List<String> fields = strings(o, "partitionFields");
        return fields.isEmpty() ? "" : tab(i) + "partitionFields: [" + String.join(", ", fields) + "];\n";
    }

    private static String ingestMode(String name, int i, String body) {
        return tab(i) + "ingestMode: " + name + "\n" + tab(i) + "{\n" + body + tab(i) + "}\n";
    }

    private static String auditing(PPersistenceNode mode, int i) {
        return apply(AUDITINGS, child(mode, "auditing"), i, "auditing");
    }

    private static String transaction(PPersistenceNode mode, int i) {
        return apply(TRANSACTION_MILESTONINGS, child(mode, "transactionMilestoning"), i, "transaction milestoning");
    }

    private static String validity(PPersistenceNode mode, int i) {
        return apply(VALIDITY_MILESTONINGS, child(mode, "validityMilestoning"), i, "validity milestoning");
    }

    private static String merge(PPersistenceNode mode, int i) {
        return apply(MERGE_STRATEGIES, child(mode, "mergeStrategy"), i, "merge strategy");
    }

    /** {@code head} then its {@code { }} block at level {@code i}. */
    private static String milestoning(String head, int i, String lines) {
        return tab(i) + head + "\n" + tab(i) + "{\n" + lines + tab(i) + "}\n";
    }

    /** A {@code key: 'value';} line one level in. */
    private static String quoted(PPersistenceNode o, String key, int i) {
        return tab(i + 1) + key + ": '" + scalar(o, key) + "';\n";
    }

    /** A {@code key: value;} line one level in. */
    private static String bare(PPersistenceNode o, String key, int i) {
        return tab(i + 1) + key + ": " + scalar(o, key) + ";\n";
    }

    private static String deleteIndicatorMerge(PPersistenceNode s, int i) {
        List<String> values = new ArrayList<>();
        for (String v : strings(s, "deleteValues")) {
            values.add(convertString(v, true));
        }
        return tab(i) + "mergeStrategy: DeleteIndicator\n" + tab(i) + "{\n"
                + tab(i + 1) + "deleteField: " + scalar(s, "deleteField") + ";\n"
                + tab(i + 1) + "deleteValues: [" + String.join(", ", values) + "];\n"
                + tab(i) + "}\n";
    }
}
