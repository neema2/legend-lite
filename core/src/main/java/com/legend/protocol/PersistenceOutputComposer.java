// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.PersistenceComposer.joined;
import static com.legend.protocol.PersistenceComposer.rule;

/**
 * A persistence's service outputs and their targets as upstream prints them ({@code HelperPersistenceComposer}'s
 * service output, dataset type, partitioning, action indicator and deduplication visitors, and
 * {@code HelperPersistenceRelationalComposer}'s relational target and temporality).
 */
final class PersistenceOutputComposer {

    /** A printer of one shape at an indentation level. */
    private interface Printer extends BiFunction<Json.Obj, Integer, String> {
    }

    private static final Map<String, Printer> SERVICE_OUTPUTS = Map.of(
            "graphFetchServiceOutput", (o, i) -> tab(i) + path(o.getObj("path")) + "\n" + tab(i) + "{\n"
                    + keys(paths(o, "keys"), i + 1) + datasetType(o, i + 1) + deduplication(o, i + 1) + tab(i) + "}\n",
            "tdsServiceOutput", (o, i) -> tab(i) + "TDS\n" + tab(i) + "{\n"
                    + keys(joined(o, "keys"), i + 1) + datasetType(o, i + 1) + deduplication(o, i + 1) + tab(i) + "}\n");

    private static final Map<String, Printer> DATASET_TYPES = Map.of(
            "snapshot", (o, i) -> tab(i) + "datasetType: Snapshot\n" + tab(i) + "{\n"
                    + optional(PersistenceOutputComposer.PARTITIONINGS, objOr(o, "partitioning"), i + 1, "partitioning") + tab(i) + "}\n",
            "delta", (o, i) -> tab(i) + "datasetType: Delta\n" + tab(i) + "{\n"
                    + optional(PersistenceOutputComposer.ACTION_INDICATORS, objOr(o, "actionIndicator"), i + 1, "action indicator") + tab(i) + "}\n");

    private static final Map<String, Printer> PARTITIONINGS = Map.of(
            "noPartitioning", (o, i) -> tab(i) + "partitioning: None\n" + tab(i) + "{\n"
                    + optional(PersistenceOutputComposer.EMPTY_DATASET_HANDLINGS, objOr(o, "emptyDatasetHandling"), i + 1, "empty dataset handling") + tab(i) + "}\n",
            "fieldBasedForGraphFetch", (o, i) -> fieldBased(paths(o, "partitionFieldPaths"), i),
            "fieldBasedForTds", (o, i) -> fieldBased(joined(o, "partitionFields"), i));

    private static final Map<String, Printer> EMPTY_DATASET_HANDLINGS = Map.of(
            "noOp", (o, i) -> tab(i) + "emptyDatasetHandling: NoOp;\n",
            "deleteTargetDataset", (o, i) -> tab(i) + "emptyDatasetHandling: DeleteTargetData;\n");

    private static final Map<String, Printer> ACTION_INDICATORS = Map.of(
            "noActionIndicator", (o, i) -> tab(i) + "actionIndicator: None;\n",
            "deleteIndicatorForGraphFetch", (o, i) -> deleteIndicator(path(o.getObj("deleteFieldPath")), o, i),
            "deleteIndicatorForTds", (o, i) -> deleteIndicator(o.getString("deleteField"), o, i));

    private static final Map<String, Printer> DEDUPLICATIONS = Map.of(
            "noDeduplication", (o, i) -> tab(i) + "deduplication: None;\n",
            "anyVersion", (o, i) -> tab(i) + "deduplication: AnyVersion;\n",
            "maxVersionForGraphFetch", (o, i) -> maxVersion(path(o.getObj("versionFieldPath")), i),
            "maxVersionForTds", (o, i) -> maxVersion(o.getString("versionField"), i));

    private PersistenceOutputComposer() {
    }

    static String serviceOutput(Json.Obj o, int i) {
        return rule(SERVICE_OUTPUTS, o, "service output").apply(o, i);
    }

    private static String optional(Map<String, Printer> table, @com.legend.base.Nullable Json.Obj o, int i, String what) {
        return o == null ? "" : rule(table, o, what).apply(o, i);
    }

    private static String datasetType(Json.Obj output, int i) {
        Json.Obj type = output.getObj("datasetType");
        return rule(DATASET_TYPES, type, "dataset type").apply(type, i);
    }

    private static String deduplication(Json.Obj output, int i) {
        return optional(DEDUPLICATIONS, objOr(output, "deduplication"), i, "deduplication");
    }

    private static String keys(String keys, int i) {
        return tab(i) + "keys:\n" + tab(i) + "[\n" + tab(i + 1) + keys + "\n" + tab(i) + "]\n";
    }

    private static String fieldBased(String fields, int i) {
        return tab(i) + "partitioning: FieldBased\n" + tab(i) + "{\n"
                + tab(i + 1) + "partitionFields:\n" + tab(i + 1) + "[\n" + tab(i + 2) + fields + "\n" + tab(i + 1) + "];\n"
                + tab(i) + "}\n";
    }

    private static String deleteIndicator(String field, Json.Obj o, int i) {
        List<String> values = new ArrayList<>();
        for (String v : o.getStringArrayOr("deleteValues", List.of())) {
            values.add(convertString(v, true));
        }
        return tab(i) + "actionIndicator: DeleteIndicator\n" + tab(i) + "{\n"
                + tab(i + 1) + "deleteField: " + field + ";\n"
                + tab(i + 1) + "deleteValues: [" + String.join(", ", values) + "];\n"
                + tab(i) + "}\n";
    }

    private static String maxVersion(String field, int i) {
        return tab(i) + "deduplication: MaxVersion\n" + tab(i) + "{\n" + tab(i + 1) + "versionField: " + field + ";\n" + tab(i) + "}\n";
    }

    private static String paths(Json.Obj o, String key) {
        List<String> out = new ArrayList<>();
        for (Json.Obj p : objs(o, key)) {
            out.add(path(p));
        }
        return String.join(", ", out);
    }

    /** {@code ServiceOutputComposer.renderPath}: the start type as written, the properties, the name. */
    static String path(Json.Obj path) {
        List<String> elements = new ArrayList<>();
        for (Json.Obj e : objs(path, "path")) {
            if (!"propertyPath".equals(Composing.type(e))) {
                throw Composing.refused("no composer rule for a path element of _type '" + Composing.type(e) + "'");
            }
            List<Json.Node> params = items(e, "parameters");
            List<String> ps = new ArrayList<>();
            for (Json.Node p : params) {
                ps.add(Composing.valueSpecification(p));
            }
            elements.add(e.getString("property") + (params.size() > 1 ? "(" + String.join(", ", ps) + ")" : ""));
        }
        String name = str(path, "name");
        return "#/" + path.getString("startType") + (elements.isEmpty() ? "" : "/" + String.join("/", elements))
                + (name == null || name.isEmpty() ? "" : "!" + name) + "#";
    }

    // ---------------------------------------------------------------------
    // Targets (the relational extension's)
    // ---------------------------------------------------------------------

    private static final Map<String, Printer> TEMPORALITIES = Map.of(
            "none", (o, i) -> "None\n" + tab(i) + "{\n"
                    + optionalPrefixed("auditing", PersistenceOutputComposer.TEMPORAL_AUDITINGS, objOr(o, "auditing"), i + 1)
                    + prefixed("updatesHandling", PersistenceOutputComposer.UPDATES_HANDLINGS, o.getObj("updatesHandling"), i + 1) + tab(i) + "}\n",
            "unitemporalTemporality", (o, i) -> "Unitemporal\n" + tab(i) + "{\n"
                    + prefixed("processingDimension", PersistenceOutputComposer.PROCESSING_DIMENSIONS, o.getObj("processingDimension"), i + 1) + tab(i) + "}\n",
            "bitemporalTemporality", (o, i) -> "Bitemporal\n" + tab(i) + "{\n"
                    + prefixed("processingDimension", PersistenceOutputComposer.PROCESSING_DIMENSIONS, o.getObj("processingDimension"), i + 1)
                    + prefixed("sourceDerivedDimension", PersistenceOutputComposer.SOURCE_DERIVED_DIMENSIONS, o.getObj("sourceDerivedDimension"), i + 1)
                    + tab(i) + "}\n");

    private static final Map<String, Printer> TEMPORAL_AUDITINGS = Map.of(
            "auditingDateTime", (o, i) -> block("DateTime", i, line(i, "dateTimeName", o.getString("auditingDateTimeName"))),
            "noAuditing", (o, i) -> "None;\n");

    private static final Map<String, Printer> UPDATES_HANDLINGS = Map.of(
            "appendOnly", (o, i) -> "AppendOnly\n" + tab(i) + "{\n"
                    + prefixed("appendStrategy", PersistenceOutputComposer.APPEND_STRATEGIES, o.getObj("appendStrategy"), i + 1) + tab(i) + "}\n",
            "overwrite", (o, i) -> "Overwrite;\n");

    private static final Map<String, Printer> APPEND_STRATEGIES = Map.of(
            "allowDuplicates", (o, i) -> "AllowDuplicates;\n",
            "failOnDuplicates", (o, i) -> "FailOnDuplicates;\n",
            "filterDuplicates", (o, i) -> "FilterDuplicates;\n");

    private static final Map<String, Printer> PROCESSING_DIMENSIONS = Map.of(
            "batchId", (o, i) -> block("BatchId", i, line(i, "batchIdIn", o.getString("batchIdIn")) + line(i, "batchIdOut", o.getString("batchIdOut"))),
            "processingTime", (o, i) -> block("DateTime", i, line(i, "dateTimeIn", o.getString("timeIn")) + line(i, "dateTimeOut", o.getString("timeOut"))),
            "batchIdAndProcessingTime", (o, i) -> block("BatchIdAndDateTime", i,
                    line(i, "batchIdIn", o.getString("batchIdIn")) + line(i, "batchIdOut", o.getString("batchIdOut"))
                            + line(i, "dateTimeIn", o.getString("timeIn")) + line(i, "dateTimeOut", o.getString("timeOut"))));

    private static final Map<String, Printer> SOURCE_DERIVED_DIMENSIONS = Map.of(
            "sourceDerivedTime", (o, i) -> block("DateTime", i,
                    line(i, "dateTimeStart", o.getString("timeStart")) + line(i, "dateTimeEnd", o.getString("timeEnd"))
                            + prefixed("sourceFields", PersistenceOutputComposer.SOURCE_TIME_FIELDS, o.getObj("sourceTimeFields"), i + 1)));

    private static final Map<String, Printer> SOURCE_TIME_FIELDS = Map.of(
            "sourceTimeStart", (o, i) -> block("Start", i, line(i, "startField", o.getString("startField"))),
            "sourceTimeStartAndEnd", (o, i) -> block("StartAndEnd", i,
                    line(i, "startField", o.getString("startField")) + line(i, "endField", o.getString("endField"))));

    /** {@code renderPersistenceTarget}: an absent target is an empty block. */
    static String target(@com.legend.base.Nullable Json.Obj target, int i) {
        if (target == null) {
            return tab(i) + "{\n" + tab(i) + "}";
        }
        if (!"relationalPersistenceTarget".equals(Composing.type(target))) {
            throw Composing.refused("no composer rule for a persistence target of _type '" + Composing.type(target) + "'");
        }
        return tab(i) + "Relational\n" + tab(i) + "#{\n"
                + tab(i + 1) + "table: " + target.getString("table") + ";\n"
                + tab(i + 1) + "database: " + target.getObj("database").getString("path") + ";\n"
                + prefixed("temporality", TEMPORALITIES, target.getObj("temporality"), i + 1)
                + tab(i) + "}#";
    }

    /** {@code key: } then the shape's own text, which carries its line ends. */
    private static String prefixed(String key, Map<String, Printer> table, Json.Obj o, int i) {
        return tab(i) + key + ": " + rule(table, o, key).apply(o, i);
    }

    private static String optionalPrefixed(String key, Map<String, Printer> table, @com.legend.base.Nullable Json.Obj o, int i) {
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
