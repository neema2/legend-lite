// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.PersistenceComposer.rule;

/**
 * A persistence's (v1) persister as upstream prints it ({@code HelperPersistenceComposer}'s persister, sink,
 * target shape, deduplication strategy, ingest mode, auditing, milestoning and merge strategy visitors).
 */
final class PersistencePersisterComposer {

    /** A printer of one shape at an indentation level. */
    private interface Printer extends BiFunction<Json.Obj, Integer, String> {
    }

    private static final Map<String, Printer> PERSISTERS = Map.of(
            "streamingPersister", (p, i) -> tab(i) + "persister: Streaming\n" + tab(i) + "{\n" + sink(p, i + 1) + tab(i) + "}\n",
            "batchPersister", (p, i) -> tab(i) + "persister: Batch\n" + tab(i) + "{\n" + sink(p, i + 1)
                    + apply(PersistencePersisterComposer.INGEST_MODES, p.getObj("ingestMode"), i + 1, "ingest mode")
                    + apply(PersistencePersisterComposer.TARGET_SHAPES, p.getObj("targetShape"), i + 1, "target shape") + tab(i) + "}\n");

    private static final Map<String, Printer> SINKS = Map.of(
            "relationalSink", (s, i) -> tab(i) + "sink: Relational\n" + tab(i) + "{\n"
                    + tab(i + 1) + "database: " + s.getObj("database").getString("path") + ";\n" + tab(i) + "}\n",
            "objectStorageSink", (s, i) -> tab(i) + "sink: ObjectStorage\n" + tab(i) + "{\n"
                    + tab(i + 1) + "binding: " + s.getObj("binding").getString("path") + ";\n" + tab(i) + "}\n");

    private static final Map<String, Printer> TARGET_SHAPES = Map.of(
            "flatTarget", PersistencePersisterComposer::flatTarget,
            "multiFlatTarget", PersistencePersisterComposer::multiFlatTarget);

    private static final Map<String, Printer> DEDUPLICATION_STRATEGIES = Map.of(
            "noDeduplicationStrategy", (d, i) -> "",
            "anyVersionDeduplicationStrategy", (d, i) -> tab(i) + "deduplicationStrategy: AnyVersion;\n",
            "maxVersionDeduplicationStrategy", (d, i) -> tab(i) + "deduplicationStrategy: MaxVersion\n" + tab(i) + "{\n"
                    + tab(i + 1) + "versionField: " + d.getString("versionField") + ";\n" + tab(i) + "}\n",
            "duplicateCountDeduplicationStrategy", (d, i) -> tab(i) + "deduplicationStrategy: DuplicateCount\n" + tab(i) + "{\n"
                    + tab(i + 1) + "duplicateCountName: '" + d.getString("duplicateCountName") + "';\n" + tab(i) + "}\n");

    private static final Map<String, Printer> INGEST_MODES = Map.of(
            "nontemporalSnapshot", (m, i) -> ingestMode("NontemporalSnapshot", i, auditing(m, i + 1)),
            "unitemporalSnapshot", (m, i) -> ingestMode("UnitemporalSnapshot", i, transaction(m, i + 1)),
            "bitemporalSnapshot", (m, i) -> ingestMode("BitemporalSnapshot", i, transaction(m, i + 1) + validity(m, i + 1)),
            "nontemporalDelta", (m, i) -> ingestMode("NontemporalDelta", i, merge(m, i + 1) + auditing(m, i + 1)),
            "unitemporalDelta", (m, i) -> ingestMode("UnitemporalDelta", i, merge(m, i + 1) + transaction(m, i + 1)),
            "bitemporalDelta", (m, i) -> ingestMode("BitemporalDelta", i, merge(m, i + 1) + transaction(m, i + 1) + validity(m, i + 1)),
            "appendOnly", (m, i) -> ingestMode("AppendOnly", i, auditing(m, i + 1)
                    + tab(i + 1) + "filterDuplicates: " + RelationalConnectionComposer.raw(m.get("filterDuplicates")) + ";\n"));

    private static final Map<String, Printer> AUDITINGS = Map.of(
            "noAuditing", (a, i) -> tab(i) + "auditing: None;\n",
            "dateTimeAuditing", (a, i) -> tab(i) + "auditing: DateTime\n" + tab(i) + "{\n"
                    + tab(i + 1) + "dateTimeName: '" + a.getString("dateTimeName") + "';\n" + tab(i) + "}\n");

    private static final Map<String, Printer> TRANSACTION_MILESTONINGS = Map.of(
            "batchIdTransactionMilestoning", (t, i) -> milestoning("transactionMilestoning: BatchId", i,
                    quoted(t, "batchIdInName", i) + quoted(t, "batchIdOutName", i)),
            "dateTimeTransactionMilestoning", (t, i) -> milestoning("transactionMilestoning: DateTime", i,
                    quoted(t, "dateTimeInName", i) + quoted(t, "dateTimeOutName", i) + optional(PersistencePersisterComposer.TRANSACTION_DERIVATIONS, objOr(t, "derivation"), i + 1)),
            "batchIdAndDateTimeTransactionMilestoning", (t, i) -> milestoning("transactionMilestoning: BatchIdAndDateTime", i,
                    quoted(t, "batchIdInName", i) + quoted(t, "batchIdOutName", i) + quoted(t, "dateTimeInName", i)
                            + quoted(t, "dateTimeOutName", i) + optional(PersistencePersisterComposer.TRANSACTION_DERIVATIONS, objOr(t, "derivation"), i + 1)));

    private static final Map<String, Printer> TRANSACTION_DERIVATIONS = Map.of(
            "sourceSpecifiesInDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesInDateTime", i,
                    bare(d, "sourceDateTimeInField", i)),
            "sourceSpecifiesInAndOutDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesInAndOutDateTime", i,
                    bare(d, "sourceDateTimeInField", i) + bare(d, "sourceDateTimeOutField", i)));

    private static final Map<String, Printer> VALIDITY_MILESTONINGS = Map.of(
            "dateTimeValidityMilestoning", (v, i) -> milestoning("validityMilestoning: DateTime", i,
                    quoted(v, "dateTimeFromName", i) + quoted(v, "dateTimeThruName", i)
                            + apply(PersistencePersisterComposer.VALIDITY_DERIVATIONS, v.getObj("derivation"), i + 1, "validity derivation")));

    private static final Map<String, Printer> VALIDITY_DERIVATIONS = Map.of(
            "sourceSpecifiesFromDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesFromDateTime", i,
                    bare(d, "sourceDateTimeFromField", i)),
            "sourceSpecifiesFromAndThruDateTime", (d, i) -> milestoning("derivation: SourceSpecifiesFromAndThruDateTime", i,
                    bare(d, "sourceDateTimeFromField", i) + bare(d, "sourceDateTimeThruField", i)));

    private static final Map<String, Printer> MERGE_STRATEGIES = Map.of(
            "noDeletesMergeStrategy", (s, i) -> tab(i) + "mergeStrategy: NoDeletes;\n",
            "deleteIndicatorMergeStrategy", PersistencePersisterComposer::deleteIndicatorMerge);

    private PersistencePersisterComposer() {
    }

    static String persister(Json.Obj persister, int i) {
        return apply(PERSISTERS, persister, i, "persister");
    }

    private static String apply(Map<String, Printer> table, Json.Obj o, int i, String what) {
        return rule(table, o, what).apply(o, i);
    }

    private static String optional(Map<String, Printer> table, @com.legend.base.Nullable Json.Obj o, int i) {
        return o == null ? "" : apply(table, o, i, "derivation");
    }

    private static String sink(Json.Obj persister, int i) {
        return apply(SINKS, persister.getObj("sink"), i, "sink");
    }

    private static String flatTarget(Json.Obj t, int i) {
        String modelClass = str(t, "modelClass");
        return tab(i) + "targetShape: Flat\n" + tab(i) + "{\n"
                + (modelClass == null ? "" : tab(i + 1) + "modelClass: " + modelClass + ";\n")
                + tab(i + 1) + "targetName: " + convertString(t.getString("targetName"), true) + ";\n"
                + partitionFields(t, i + 1)
                + apply(DEDUPLICATION_STRATEGIES, t.getObj("deduplicationStrategy"), i + 1, "deduplication strategy")
                + tab(i) + "}\n";
    }

    private static String multiFlatTarget(Json.Obj t, int i) {
        List<Json.Obj> parts = objs(t, "parts");
        StringBuilder b = new StringBuilder();
        for (int p = 0; p < parts.size(); p++) {
            Json.Obj part = parts.get(p);
            b.append(tab(i + 2)).append("{\n")
                    .append(tab(i + 3)).append("modelProperty: ").append(part.getString("modelProperty")).append(";\n")
                    .append(tab(i + 3)).append("targetName: ").append(convertString(part.getString("targetName"), true)).append(";\n")
                    .append(partitionFields(part, i + 3))
                    .append(apply(DEDUPLICATION_STRATEGIES, part.getObj("deduplicationStrategy"), i + 3, "deduplication strategy"))
                    .append(tab(i + 2)).append(p < parts.size() - 1 ? "},\n" : "}\n");
        }
        return tab(i) + "targetShape: MultiFlat\n" + tab(i) + "{\n"
                + tab(i + 1) + "modelClass: " + t.getString("modelClass") + ";\n"
                + tab(i + 1) + "transactionScope: " + t.getString("transactionScope") + ";\n"
                + tab(i + 1) + "parts:\n" + tab(i + 1) + "[\n" + b + tab(i + 1) + "];\n"
                + tab(i) + "}\n";
    }

    private static String partitionFields(Json.Obj o, int i) {
        List<String> fields = o.getStringArrayOr("partitionFields", List.of());
        return fields.isEmpty() ? "" : tab(i) + "partitionFields: [" + String.join(", ", fields) + "];\n";
    }

    private static String ingestMode(String name, int i, String body) {
        return tab(i) + "ingestMode: " + name + "\n" + tab(i) + "{\n" + body + tab(i) + "}\n";
    }

    private static String auditing(Json.Obj mode, int i) {
        return apply(AUDITINGS, mode.getObj("auditing"), i, "auditing");
    }

    private static String transaction(Json.Obj mode, int i) {
        return apply(TRANSACTION_MILESTONINGS, mode.getObj("transactionMilestoning"), i, "transaction milestoning");
    }

    private static String validity(Json.Obj mode, int i) {
        return apply(VALIDITY_MILESTONINGS, mode.getObj("validityMilestoning"), i, "validity milestoning");
    }

    private static String merge(Json.Obj mode, int i) {
        return apply(MERGE_STRATEGIES, mode.getObj("mergeStrategy"), i, "merge strategy");
    }

    /** {@code head} then its {@code { }} block at level {@code i}. */
    private static String milestoning(String head, int i, String lines) {
        return tab(i) + head + "\n" + tab(i) + "{\n" + lines + tab(i) + "}\n";
    }

    /** A {@code key: 'value';} line one level in. */
    private static String quoted(Json.Obj o, String key, int i) {
        return tab(i + 1) + key + ": '" + o.getString(key) + "';\n";
    }

    /** A {@code key: value;} line one level in. */
    private static String bare(Json.Obj o, String key, int i) {
        return tab(i + 1) + key + ": " + o.getString(key) + ";\n";
    }

    private static String deleteIndicatorMerge(Json.Obj s, int i) {
        List<String> values = new ArrayList<>();
        for (String v : s.getStringArrayOr("deleteValues", List.of())) {
            values.add(convertString(v, true));
        }
        return tab(i) + "mergeStrategy: DeleteIndicator\n" + tab(i) + "{\n"
                + tab(i + 1) + "deleteField: " + s.getString("deleteField") + ";\n"
                + tab(i + 1) + "deleteValues: [" + String.join(", ", values) + "];\n"
                + tab(i) + "}\n";
    }
}
