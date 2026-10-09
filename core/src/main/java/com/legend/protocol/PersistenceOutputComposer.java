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
import static com.legend.protocol.PersistenceComposer.field;
import static com.legend.protocol.PersistenceComposer.fields;
import static com.legend.protocol.PersistenceComposer.optChild;
import static com.legend.protocol.PersistenceComposer.pointer;
import static com.legend.protocol.PersistenceComposer.rule;
import static com.legend.protocol.PersistenceComposer.scalar;
import static com.legend.protocol.PersistenceComposer.strings;

/**
 * A persistence's service outputs and their targets as upstream prints them ({@code HelperPersistenceComposer}'s
 * service output, dataset type, partitioning, action indicator and deduplication visitors, and
 * {@code HelperPersistenceRelationalComposer}'s relational target and temporality) -- over the persistence tree
 * ({@link PPersistenceNode}; the protocol program's leg 2, step 3). A graphFetch output's fields are path values
 * where a TDS output's are names: {@link PersistenceComposer#field} and {@link PersistenceComposer#fields} print
 * either.
 */
final class PersistenceOutputComposer {

    /** A printer of one kind of node at an indentation level. */
    private interface Printer extends BiFunction<PPersistenceNode, Integer, String> {
    }

    /** A graphFetch output is headed by its path ({@code #path}); a TDS output by its keyword. */
    private static final Map<String, Printer> SERVICE_OUTPUTS = Map.of(
            "#path", (o, i) -> tab(i) + PersistenceComposer.path(head(o)) + "\n" + tab(i) + "{\n"
                    + keys(fields(o, "keys", "keys"), i + 1) + datasetType(o, i + 1) + deduplication(o, i + 1) + tab(i) + "}\n",
            "TDS", (o, i) -> tab(i) + "TDS\n" + tab(i) + "{\n"
                    + keys(String.join(", ", strings(o, "keys")), i + 1) + datasetType(o, i + 1) + deduplication(o, i + 1)
                    + tab(i) + "}\n");

    private static final Map<String, Printer> DATASET_TYPES = Map.of(
            "Snapshot", (o, i) -> tab(i) + "datasetType: Snapshot\n" + tab(i) + "{\n"
                    + optional(PersistenceOutputComposer.PARTITIONINGS, optChild(o, "partitioning"), i + 1, "partitioning") + tab(i) + "}\n",
            "Delta", (o, i) -> tab(i) + "datasetType: Delta\n" + tab(i) + "{\n"
                    + optional(PersistenceOutputComposer.ACTION_INDICATORS, optChild(o, "actionIndicator"), i + 1, "action indicator") + tab(i) + "}\n");

    private static final Map<String, Printer> PARTITIONINGS = Map.of(
            "None", (o, i) -> tab(i) + "partitioning: None\n" + tab(i) + "{\n"
                    + optional(PersistenceOutputComposer.EMPTY_DATASET_HANDLINGS, optChild(o, "emptyDatasetHandling"), i + 1, "empty dataset handling") + tab(i) + "}\n",
            "FieldBased", (o, i) -> fieldBased(fields(o, "partitionFields", "partitionFieldPaths"), i));

    private static final Map<String, Printer> EMPTY_DATASET_HANDLINGS = Map.of(
            "NoOp", (o, i) -> tab(i) + "emptyDatasetHandling: NoOp;\n",
            "DeleteTargetData", (o, i) -> tab(i) + "emptyDatasetHandling: DeleteTargetData;\n");

    private static final Map<String, Printer> ACTION_INDICATORS = Map.of(
            "None", (o, i) -> tab(i) + "actionIndicator: None;\n",
            "DeleteIndicator", PersistenceOutputComposer::deleteIndicator);

    private static final Map<String, Printer> DEDUPLICATIONS = Map.of(
            "None", (o, i) -> tab(i) + "deduplication: None;\n",
            "AnyVersion", (o, i) -> tab(i) + "deduplication: AnyVersion;\n",
            "MaxVersion", (o, i) -> tab(i) + "deduplication: MaxVersion\n" + tab(i) + "{\n" + tab(i + 1) + "versionField: "
                    + field(o, "versionField") + ";\n" + tab(i) + "}\n");

    private PersistenceOutputComposer() {
    }

    static String serviceOutput(PPersistenceNode o, int i) {
        return rule(SERVICE_OUTPUTS, o, "service output").apply(o, i);
    }

    private static com.legend.protocol.spec.ValueSpecification head(PPersistenceNode o) {
        if (o.headPath() == null) {
            throw Composing.refused("a graphFetch service output without its path");
        }
        return o.headPath();
    }

    private static String optional(Map<String, Printer> table, @com.legend.base.Nullable PPersistenceNode o, int i, String what) {
        return o == null ? "" : rule(table, o, what).apply(o, i);
    }

    private static String datasetType(PPersistenceNode output, int i) {
        PPersistenceNode type = child(output, "datasetType");
        return rule(DATASET_TYPES, type, "dataset type").apply(type, i);
    }

    private static String deduplication(PPersistenceNode output, int i) {
        return optional(DEDUPLICATIONS, optChild(output, "deduplication"), i, "deduplication");
    }

    private static String keys(String keys, int i) {
        return tab(i) + "keys:\n" + tab(i) + "[\n" + tab(i + 1) + keys + "\n" + tab(i) + "]\n";
    }

    private static String fieldBased(String fields, int i) {
        return tab(i) + "partitioning: FieldBased\n" + tab(i) + "{\n"
                + tab(i + 1) + "partitionFields:\n" + tab(i + 1) + "[\n" + tab(i + 2) + fields + "\n" + tab(i + 1) + "];\n"
                + tab(i) + "}\n";
    }

    private static String deleteIndicator(PPersistenceNode o, int i) {
        List<String> values = new ArrayList<>();
        for (String v : strings(o, "deleteValues")) {
            values.add(convertString(v, true));
        }
        return tab(i) + "actionIndicator: DeleteIndicator\n" + tab(i) + "{\n"
                + tab(i + 1) + "deleteField: " + field(o, "deleteField") + ";\n"
                + tab(i + 1) + "deleteValues: [" + String.join(", ", values) + "];\n"
                + tab(i) + "}\n";
    }

    // ---------------------------------------------------------------------
    // Targets (the relational extension's)
    // ---------------------------------------------------------------------

    private static final Map<String, Printer> TEMPORALITIES = Map.of(
            "None", (o, i) -> "None\n" + tab(i) + "{\n"
                    + optionalPrefixed("auditing", PersistenceOutputComposer.TEMPORAL_AUDITINGS, optChild(o, "auditing"), i + 1)
                    + prefixed("updatesHandling", PersistenceOutputComposer.UPDATES_HANDLINGS, child(o, "updatesHandling"), i + 1) + tab(i) + "}\n",
            "Unitemporal", (o, i) -> "Unitemporal\n" + tab(i) + "{\n"
                    + prefixed("processingDimension", PersistenceOutputComposer.PROCESSING_DIMENSIONS, child(o, "processingDimension"), i + 1) + tab(i) + "}\n",
            "Bitemporal", (o, i) -> "Bitemporal\n" + tab(i) + "{\n"
                    + prefixed("processingDimension", PersistenceOutputComposer.PROCESSING_DIMENSIONS, child(o, "processingDimension"), i + 1)
                    + prefixed("sourceDerivedDimension", PersistenceOutputComposer.SOURCE_DERIVED_DIMENSIONS, child(o, "sourceDerivedDimension"), i + 1)
                    + tab(i) + "}\n");

    private static final Map<String, Printer> TEMPORAL_AUDITINGS = Map.of(
            "DateTime", (o, i) -> block("DateTime", i, line(i, "dateTimeName", scalar(o, "dateTimeName"))),
            "None", (o, i) -> "None;\n");

    private static final Map<String, Printer> UPDATES_HANDLINGS = Map.of(
            "AppendOnly", (o, i) -> "AppendOnly\n" + tab(i) + "{\n"
                    + prefixed("appendStrategy", PersistenceOutputComposer.APPEND_STRATEGIES, child(o, "appendStrategy"), i + 1) + tab(i) + "}\n",
            "Overwrite", (o, i) -> "Overwrite;\n");

    private static final Map<String, Printer> APPEND_STRATEGIES = Map.of(
            "AllowDuplicates", (o, i) -> "AllowDuplicates;\n",
            "FailOnDuplicates", (o, i) -> "FailOnDuplicates;\n",
            "FilterDuplicates", (o, i) -> "FilterDuplicates;\n");

    private static final Map<String, Printer> PROCESSING_DIMENSIONS = Map.of(
            "BatchId", (o, i) -> block("BatchId", i, line(i, "batchIdIn", scalar(o, "batchIdIn"))
                    + line(i, "batchIdOut", scalar(o, "batchIdOut"))),
            "DateTime", (o, i) -> block("DateTime", i, line(i, "dateTimeIn", scalar(o, "dateTimeIn"))
                    + line(i, "dateTimeOut", scalar(o, "dateTimeOut"))),
            "BatchIdAndDateTime", (o, i) -> block("BatchIdAndDateTime", i,
                    line(i, "batchIdIn", scalar(o, "batchIdIn")) + line(i, "batchIdOut", scalar(o, "batchIdOut"))
                            + line(i, "dateTimeIn", scalar(o, "dateTimeIn")) + line(i, "dateTimeOut", scalar(o, "dateTimeOut"))));

    private static final Map<String, Printer> SOURCE_DERIVED_DIMENSIONS = Map.of(
            "DateTime", (o, i) -> block("DateTime", i,
                    line(i, "dateTimeStart", scalar(o, "dateTimeStart")) + line(i, "dateTimeEnd", scalar(o, "dateTimeEnd"))
                            + prefixed("sourceFields", PersistenceOutputComposer.SOURCE_TIME_FIELDS, child(o, "sourceFields"), i + 1)));

    private static final Map<String, Printer> SOURCE_TIME_FIELDS = Map.of(
            "Start", (o, i) -> block("Start", i, line(i, "startField", scalar(o, "startField"))),
            "StartAndEnd", (o, i) -> block("StartAndEnd", i,
                    line(i, "startField", scalar(o, "startField")) + line(i, "endField", scalar(o, "endField"))));

    /** {@code renderPersistenceTarget}: an unspelled target is an empty block. */
    static String target(PPersistenceNode target, int i) {
        if ("__empty__".equals(target.kind())) {
            return tab(i) + "{\n" + tab(i) + "}";
        }
        if (!"Relational".equals(target.kind())) {
            throw Composing.refused("no composer rule for a persistence target of kind '" + target.kind() + "'");
        }
        return tab(i) + "Relational\n" + tab(i) + "#{\n"
                + tab(i + 1) + "table: " + scalar(target, "table") + ";\n"
                + tab(i + 1) + "database: " + pointer(target, "database") + ";\n"
                + prefixed("temporality", TEMPORALITIES, child(target, "temporality"), i + 1)
                + tab(i) + "}#";
    }

    /** {@code key: } then the shape's own text, which carries its line ends. */
    private static String prefixed(String key, Map<String, Printer> table, PPersistenceNode o, int i) {
        return tab(i) + key + ": " + rule(table, o, key).apply(o, i);
    }

    private static String optionalPrefixed(String key, Map<String, Printer> table, @com.legend.base.Nullable PPersistenceNode o, int i) {
        return o == null ? "" : prefixed(key, table, o, i);
    }

    /** {@code Keyword} then its {@code { }} block at level {@code i}. */
    private static String block(String keyword, int i, String lines) {
        return keyword + "\n" + tab(i) + "{\n" + lines + tab(i) + "}\n";
    }

    /** One {@code key: value;} line inside a block at level {@code i}. */
    private static String line(int i, String key, String value) {
        return tab(i + 1) + key + ": " + value + ";\n";
    }
}
